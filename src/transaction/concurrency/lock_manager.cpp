/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

bool LockManager::isLockCompatible(const LockRequest *iter, const LockMode &target_lock_mode) {
    switch (target_lock_mode) {
        case LockMode::INTENTION_SHARED:
            if(iter->lock_mode_ == LockMode::EXLUCSIVE){
                return false;
            }
            break;
        case LockMode::INTENTION_EXCLUSIVE:
            if(iter->lock_mode_ != LockMode::INTENTION_SHARED && iter->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
                return false;
            }
            break;
        case LockMode::SHARED:
            if(iter->lock_mode_ != LockMode::INTENTION_SHARED && iter->lock_mode_ != LockMode::SHARED){
                return false;
            }
            break;
        case LockMode::S_IX:
            if(iter->lock_mode_ != LockMode::INTENTION_SHARED) {
                return false;
            }
            break;
        case LockMode::EXLUCSIVE:
            return false;
        default:
            return false;
        }
    return true;
}

/// @brief 用于对同一个事务上的加锁请求进行锁升级的兼容性判断
/// @param iter 锁请求的指针，加锁请求的事务id=该指针的事务id
/// @param upgrade_lock_mode 新来的加锁模式
/// @param return_lock_mode 返回需要更新的锁请求模式，例如IX + S -> SIX
/// @return 是否需要锁升级，如果返回为false，则不需要更改iter
bool LockManager::isUpdateCompatible(const LockRequest *iter, const LockMode &upgrade_lock_mode, LockMode* return_lock_mode) {
    switch (iter->lock_mode_) {
        case LockMode::INTENTION_SHARED:
            if(upgrade_lock_mode == LockMode::INTENTION_SHARED){
                return false;
            }
            // IS + IX = IX, IS + S = S, IS + X = X
            *return_lock_mode = upgrade_lock_mode;
            break;
        case LockMode::SHARED:
            if(upgrade_lock_mode == LockMode::EXLUCSIVE) {
                // S + X = X
                *return_lock_mode = upgrade_lock_mode;
            }else if(upgrade_lock_mode == LockMode::INTENTION_EXCLUSIVE){
                // S + IX = SIX
                *return_lock_mode = LockMode::S_IX;
            }else{
                return false;
            }
            break;
        case LockMode::INTENTION_EXCLUSIVE:
            if(upgrade_lock_mode == LockMode::EXLUCSIVE) {
                // IX + X = X
                *return_lock_mode = upgrade_lock_mode;
            }else if(upgrade_lock_mode == LockMode::SHARED){
                // IX + S= SIX
                *return_lock_mode = LockMode::S_IX;
            }else{
                return false;
            }
            break;
        case LockMode::S_IX:
            if(upgrade_lock_mode != LockMode::EXLUCSIVE){
                return false;
            }
            // SIX + X = X
            *return_lock_mode = upgrade_lock_mode;
            break;
        case LockMode::EXLUCSIVE:
            return false;
            break;
        default:
            return false;
        }
    return true;
}

bool LockManager::checkSameTxnLockRequest(Transaction *txn, LockRequestQueue *request_queue, const LockMode target_lock_mode, std::unique_lock<std::mutex> &queue_lock){
    for(auto &iter : request_queue->request_queue_){
        if( iter.txn_id_ == txn->get_transaction_id() ){ 
            LockMode return_lock_mode;
            bool need_upgrade = isUpdateCompatible(&iter, target_lock_mode, &return_lock_mode);
            //如果当前事务正在或已经申请模式相同或更低级的锁
            if(need_upgrade == false && iter.granted_ == true){
                // 已经获取锁, 上锁成功
                return true; 
            }
            else if(need_upgrade == false && iter.granted_ == false){
                //正在申请锁, 等待这个请求申请成功后返回
                int curAttemp = 0; 
                while(curAttemp <= MaxAttempt){
                    if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
                    if(!checkQueueCompatible(request_queue, &iter)){
                        curAttemp++;
                        queue_lock.unlock();
                        std::this_thread::sleep_for(std::chrono::microseconds(50));
                    }else{
                        break;
                    }
                }
                if(curAttemp == MaxAttempt){
                    txn->set_state(TransactionState::ABORTED);
                }
                // request_queue->cv_.wait(queue_lock, [&request_queue, iter, &txn, &curAttemp, this]{ 
                //     if(!checkQueueCompatible(request_queue, &iter)){
                //         curAttemp++;
                //         if(curAttemp <= MaxAttempt){
                //             // 多次尝试, 最多尝试3次
                //             return false;
                //         }else{
                //             txn->set_state(TransactionState::ABORTED);
                //             return true;
                //         }
                //     }
                //     return true;
                // });
                if(txn->get_state()==TransactionState::ABORTED) 
                    throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                iter.granted_ = true;
                return true; 
            }
            //如果锁的模式不同, 则需要对锁进行升级, 首先检查是否有正在升级的锁, 如果有, 则返回升级冲突
            else if(request_queue->upgrading_ == true){
                throw TransactionAbortException (txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            else{
                //准备升级 
                request_queue->upgrading_ = true; 
                iter.lock_mode_ = return_lock_mode;
                iter.granted_ = false;
                int curAttemp = 0; 
                while(curAttemp <= MaxAttempt){
                    if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
                    if(!checkQueueCompatible(request_queue, &iter)){
                        curAttemp++;
                        queue_lock.unlock();
                        std::this_thread::sleep_for(std::chrono::microseconds(50));
                    }else{
                        break;
                    }
                }
                if(curAttemp == MaxAttempt){
                    txn->set_state(TransactionState::ABORTED);
                }
                // request_queue->cv_.wait(queue_lock, [&request_queue, iter, &txn, &curAttemp, this]{ 
                //     if(!checkQueueCompatible(request_queue, &iter)){
                //         curAttemp++;
                //         if(curAttemp <= MaxAttempt){
                //             // 多次尝试, 最多尝试3次
                //             return false;
                //         }else{
                //             txn->set_state(TransactionState::ABORTED);
                //             return true;
                //         }
                //     }
                //     return true;
                // });
                request_queue->upgrading_ = false;
                if(txn->get_state()==TransactionState::ABORTED) 
                    throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                iter.granted_ = true;
                return true;
            } 
        }
    }
    return false;
}

bool LockManager::checkQueueCompatible(const LockRequestQueue *request_queue, const LockRequest *request, txn_id_t *oldest_uncompatible_txn_id) {
    bool res = true;
    for(auto &iter : request_queue->request_queue_ ){
        if(iter.txn_id_ != request->txn_id_ && iter.granted_ == true){
            if(isLockCompatible(&iter, request->lock_mode_) == false){
                // 如果不兼容, 记录最老的txn_id
                res = false;
                *oldest_uncompatible_txn_id = std::min(*oldest_uncompatible_txn_id, iter.txn_id_);
            }
        }
    }
    return res;
}

bool LockManager::checkQueueCompatible(const LockRequestQueue *request_queue, const LockRequest *request) {
    for(auto &iter : request_queue->request_queue_ ){
        if(iter.txn_id_ != request->txn_id_ && iter.granted_ == true){
            if(isLockCompatible(&iter, request->lock_mode_) == false)
                return false;
        }
    }
    return true;
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, rid, LockDataType::RECORD);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::SHARED, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true; 
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, rid, LockDataType::RECORD);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::EXLUCSIVE, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, LockDataType::TABLE);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::SHARED, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, LockDataType::TABLE);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::EXLUCSIVE, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, LockDataType::TABLE);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::INTENTION_SHARED, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_SHARED); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(tab_fd, LockDataType::TABLE);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::INTENTION_EXCLUSIVE, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;
}

/**
 * @description: 申请索引上的间隙锁，间隙为(iid - 1 , iid)
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} index_fd 目标表的fd
 */
bool LockManager::lock_gap_on_index(Transaction* txn, const Rid& rid, int index_fd){
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(index_fd, rid, LockDataType::GAP);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    //步骤四: 
    //查找当前事务是否已经申请了目标数据项上的锁，
    //如果存在, 并且申请锁模式相同,返回申请成功
    //如果存在, 但与之锁的模式不同, 则准备升级, 并检查升级是否兼容
    if(checkSameTxnLockRequest(txn, request_queue, LockMode::SHARED, queue_lock)==true){
        if(txn->get_state() == TransactionState::ABORTED)
            return false;
        return true;
    }

    //步骤五, 如果当前事务在请求队列中没有申请该数据项的锁, 则新建请求加入队列
    //检查是否可以上锁, 否则阻塞, 使用条件变量cv来实现
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_request_ref = request_queue->request_queue_.emplace_back(lock_request);
    txn->get_lock_set()->emplace(l_id);
    int curAttemp = 0; 
    while(curAttemp <= MaxAttempt){
        if(curAttemp > 0) queue_lock.lock(); // 第一次已经lock
        if(!checkQueueCompatible(request_queue, &lock_request_ref)){
            curAttemp++;
            queue_lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }else{
            break;
        }
    }
    if(curAttemp == MaxAttempt){
        txn->set_state(TransactionState::ABORTED);
    }
    // request_queue->cv_.wait(queue_lock, [&request_queue, &lock_request, &txn, &curAttemp, this, lock_request_ref]{ 
    //     if(!checkQueueCompatible(request_queue, &lock_request_ref)){
    //         curAttemp++;
    //         if(curAttemp <= MaxAttempt){
    //             // 多次尝试, 最多尝试3次
    //             return false;
    //         }else{
    //             txn->set_state(TransactionState::ABORTED);
    //             return true;
    //         }
    //     }
    //     return true;
    // });
    if(txn->get_state()==TransactionState::ABORTED) 
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
    lock_request_ref.granted_ = true;
    return true;

}

bool LockManager::try_lock_in_gap(Transaction* txn, const Rid& rid, int index_fd){
    //步骤一: 检查事务状态
    if(txn->get_state() == TransactionState::DEFAULT){
        txn->set_state(TransactionState::GROWING);
    } 
    if(txn->get_state() != TransactionState::GROWING){
        throw TransactionAbortException (txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //步骤二: 得到l_id
    LockDataId l_id(index_fd, rid, LockDataType::GAP);

    //步骤三: 通过mutex申请全局锁表
    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[l_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    for(auto &iter : request_queue->request_queue_){
        if( iter.txn_id_ != txn->get_transaction_id() ){ 
            // return false; // 有其他事务正在占用此间隙
            throw TransactionAbortException (txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION) ;
        }
    }

    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {

    std::unique_lock<std::mutex> Latch(latch_);
    LockRequestQueue* request_queue = &lock_table_[lock_data_id];
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    Latch.unlock();

    if(txn->get_state() == TransactionState::GROWING){
        txn->set_state(TransactionState::SHRINKING);
    }

    auto iter = request_queue->request_queue_.begin();
    // for(; iter != request_queue->request_queue_.end(); iter++){
    //     if((*iter).txn_id_ == txn->get_transaction_id()){
    //         break;
    //     }
    // }
    // request_queue->request_queue_.erase(iter);

    while (iter != request_queue->request_queue_.end()) {
        if ((*iter).txn_id_ == txn->get_transaction_id()) {
            iter = request_queue->request_queue_.erase(iter);  // 删除元素并返回指向下一个元素的迭代器
        } else {
            ++iter;  // 移动到下一个元素
        }
    }
    request_queue->cv_.notify_all();
    
    return true;
}