#pragma once

#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/service.hpp>
#include <cascade/service_types.hpp>
#include <cascade/utils.hpp>

#include <string>
#include <type_traits>

#ifndef NDEBUG
inline void dump_layout(const json& layout) {
    int tid = 0;
    for(const auto& pertype : layout) {
        int sidx = 0;
        for(const auto& persubgroup : pertype) {
            dbg_default_trace("subgroup={}.{},layout={}.", tid, sidx, persubgroup.dump());
            sidx++;
        }
        tid++;
    }
}
#endif  // NDEBUG

using derecho::cascade::CriticalDataPathObserver;
using derecho::cascade::ICascadeContext;

/**
 * Define the CDPO
 * @tparam CascadeType  the subgroup type
 * @tparam IS_TRIGGER   If true, this is triggered only on p2p message critical data path, otherwise, on ordered send
 *                      message critical data path.
 */
template <typename CascadeType>
class CascadeServiceCDPO : public CriticalDataPathObserver<CascadeType> {
    virtual void operator()(const uint32_t sgidx,
                            const uint32_t shidx,
                            const derecho::node_id_t sender_id,
                            const typename CascadeType::KeyType& key,
                            const typename CascadeType::ObjectType& value,
                            ICascadeContext* cascade_ctxt,
                            bool is_trigger = false) override {
        if constexpr(std::is_convertible<typename CascadeType::KeyType, std::string>::value) {
            using namespace derecho::cascade;

            auto* engine = dynamic_cast<
                    ExecutionEngine<
                            VolatileCascadeStoreWithStringKey,
                            PersistentCascadeStoreWithStringKey,
                            TriggerCascadeNoStoreWithStringKey>*>(cascade_ctxt);
            size_t pos = key.rfind(PATH_SEPARATOR);
            std::string prefix;
            if(pos != std::string::npos) {
                // important: we need to keep the trailing PATH_SEPARATOR
                prefix = key.substr(0, pos + 1);
            }
            auto handlers = engine->get_prefix_handlers(prefix);
            if(handlers.empty()) {
                return;
            }
            // filter for normal put (put/put_and_forget)
            bool new_actions = false;
            bool has_mproc_udl = false;
            {
                auto shard_members = engine->get_service_client_ref().template get_shard_members<CascadeType>(sgidx, shidx);
                bool icare = (shard_members[std::hash<std::string>{}(key) % shard_members.size()] == engine->get_service_client_ref().get_my_id());
                for(auto& per_prefix : handlers) {
                    // per_prefix.first is the matching prefix
                    // per_prefix.second is an object of prefix_entry_t
                    for(auto& dfg_ocdpos : per_prefix.second) {
                        // dfg_ocdpos.first is dfg_id
                        // dfg_ocdpos.second is a set of ocdpo info object of type prefix_ocdpo_info_t.
                        for(auto oiit = dfg_ocdpos.second.begin(); oiit != dfg_ocdpos.second.end();) {
                            if((oiit->hook != DataFlowGraph::VertexHook::BOTH) && (
                                (oiit->hook == DataFlowGraph::VertexHook::ORDERED_PUT && is_trigger) || 
                                (oiit->hook == DataFlowGraph::VertexHook::TRIGGER_PUT && !is_trigger))) {
                                // not my hook, skip it.
                                oiit = dfg_ocdpos.second.erase(oiit);
                            } else if (is_trigger) {
                                new_actions = true;
                                if (oiit->execution_environment != 
                                    DataFlowGraph::VertexExecutionEnvironment::PTHREAD) {
                                    has_mproc_udl = true;
                                }
                                oiit++;
                            } else {
                                // matched ordered put data path
                                // test dispatcher:
                                switch(oiit->shard_dispatcher) {
                                    case DataFlowGraph::VertexShardDispatcher::ONE:
                                        if(icare) {
                                            new_actions = true;
                                            oiit++;
                                        } else {
                                            oiit = dfg_ocdpos.second.erase(oiit);
                                        }
                                        break;
                                    case DataFlowGraph::VertexShardDispatcher::ALL:
                                        new_actions = true;
                                        oiit++;
                                        break;
                                    default:
                                        // unknown dispatcher.
                                        oiit = dfg_ocdpos.second.erase(oiit);
                                        break;
                                }
                            }
                        }
                    }
                }
            }
            if(!new_actions) {
                return;
            }
            // copy data TODO: test has_mproc_udl, if has_mproc_udl == true, copy it to shared space,
            // otherwise, use simple make_shared() call.
            auto value_ptr = std::make_shared<typename CascadeType::ObjectType>(value);
            // create actions
            for(auto& per_prefix : handlers) {
                // per_prefix.first is the matching prefix
                // per_prefix.second is an object of prefix_entry_t
                for(const auto& dfg_ocdpos : per_prefix.second) {
                    // dfg_ocdpos.first is dfg_id
                    // dfg_ocdpos.second is a set of ocdpo info object of type prefix_ocdpo_info_t.
                    for (const auto& oi : dfg_ocdpos.second) {
                        Action action(
                                sender_id,
                                key,
                                per_prefix.first.size(),
                                value.get_version(),
                                oi.ocdpo,  // ocdpo
                                value_ptr,
                                oi.output_map  // outputs
                        );
    
#ifdef ENABLE_EVALUATION
                        ActionPostExtraInfo apei;
                        apei.uint64_val = 0;
                        apei.info.is_trigger = is_trigger;
#endif
    
#ifdef ENABLE_EVALUATION
                        apei.info.stateful = oi.statefulness;
#endif
                    TimestampLogger::log(TLT_ACTION_POST_START,
                                         ctxt->get_service_client_ref().get_my_id(),
                                         dynamic_cast<const IHasMessageID*>(&value)->get_message_id(),
                                         get_time_ns(),
                                         apei.uint64_val);
                    /**
                     * Put the action to scheduler queue if one of the two cases:
                     * 1. This action is an entry task, and hasn't been scheduled. 
                     *     If so, post it to unscheduled_queue for scheduler to schedule the whole job instance.
                     * 2. (For JIT baseline experiment)This is a joint task, which haven't been rescheduled(num_reallocate == 0), and current worker waittime exceeds its waiting threashold.
                     *     If so, post it to unscheduled_queue to be rescheduled once the whole dependent intermediate results arrived.
                     * Current implementation requires single handler for the pathname
                    */
                    bool unscheduled_entry_task = value_ptr->get_adfg().empty() && handlers.size() == 1 && action.prefix_length != 0;
                    if(unscheduled_entry_task){
                        // REQUIRED SINGLE ENTRY TASK FOR SCHEDULER
                        ctxt->post_to_scheduler(std::move(action));
                        return;
                    }
#ifdef HAS_STATEFUL_UDL_SUPPORT
                    ctxt->post(std::move(action), std::get<1>(handler.second), is_trigger);
#else
                    ctxt->post(std::move(action), is_trigger);
#endif//HAS_STATEFUL_UDL_SUPPORT
                    TimestampLogger::log(TLT_ACTION_POST_END,
                                         ctxt->get_service_client_ref().get_my_id(),
                                         dynamic_cast<const IHasMessageID*>(&value)->get_message_id(),
                                         get_time_ns(),
                                         apei.uint64_val);
                }
            }
        }
    }
};
