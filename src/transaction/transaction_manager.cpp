/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};


void TransactionManager::ReleaseLocks(Transaction *txn){
    for(auto iter = txn->get_lock_set()->begin(); iter != txn->get_lock_set()->end(); ++iter){
        lock_manager_->unlock(txn, *iter);
    }
    return ;
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_.fetch_add(1));
        std::unique_lock<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
        lock.unlock();

        if(enable_logging){
            //写Begin日志
            BeginLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn());
            lsn_t lsn = log_manager->add_log_to_buffer(&record);
            txn->set_prev_lsn(lsn);
        }

        return txn;
    }
    else assert(0);
    return nullptr;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    if(txn->get_state() == TransactionState::ABORTED){
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::COMMIT_ABOOTED_TRANSACTION);
    }

    // Release all locks
    ReleaseLocks(txn);
    txn->get_lock_set()->clear();

    while (!txn->get_write_set()->empty()) {
        auto &item = txn->get_write_set()->back();
        txn->get_write_set()->pop_back();
        delete item;
    }
    txn->get_write_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    if(enable_logging){
        //写Commit日志
        CommitLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn());
        lsn_t lsn = log_manager->add_log_to_buffer(&record);
        txn->set_prev_lsn(lsn);
        log_manager->ForceFlush(lsn);
    }

    txn->set_state(TransactionState::COMMITTED);
    // Remove txn from txn_map
    std::unique_lock<std::mutex> l(latch_);
    txn_map.erase(txn->get_transaction_id() );

    // delete txn;
    // txn = nullptr;
    return ;
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    assert(txn->get_state() != TransactionState::COMMITTED);
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        auto &item = write_set->back();
        if(item->GetWriteType() == WType::INSERT_TUPLE){
            auto fh = sm_manager_->fhs_.at(item->GetTableName()).get();
            // 删除插入的索引
            auto tab_ = sm_manager_->db_.get_table(item->GetTableName());
            // 获取插入元素的record, 注意这里item的record是空的，因为对于插入操作，构造函数中不涉及record，需要从fh中获取
            auto Tuple = fh->get_record(item->GetRid(), nullptr);
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(item->GetTableName(), index.cols)).get();
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    memcpy(key + offset, Tuple->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(key, txn);
            }
            if(enable_logging){
                //写Delete
                DeleteLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn(), *Tuple.get(), item->GetRid(), item->GetTableName());
                lsn_t lsn = log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(lsn);
                Page* page = sm_manager_->get_bpm()->fetch_page({fh->GetFd(), item->GetRid().page_no});
                page->set_page_lsn(lsn);
                sm_manager_->get_bpm()->unpin_page({fh->GetFd(), item->GetRid().page_no}, true);
            }
            
            // TODO: 这里的nullptr后续需要替换
            fh->delete_record(item->GetRid(), nullptr);
        }
        else if(item->GetWriteType() == WType::DELETE_TUPLE){
            auto fh = sm_manager_->fhs_.at(item->GetTableName()).get();
            // 添加删除的索引
            auto tab_ = sm_manager_->db_.get_table(item->GetTableName()); 
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(item->GetTableName(), index.cols)).get();
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    memcpy(key + offset, item->GetRecord().data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->insert_entry(key, item->GetRid(), txn);
            }
            if(enable_logging){
                //写Insert
                InsertLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn(), item->GetRecord(), item->GetRid(), item->GetTableName());
                lsn_t lsn = log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(lsn);
                Page* page = sm_manager_->get_bpm()->fetch_page({fh->GetFd(), item->GetRid().page_no});
                page->set_page_lsn(lsn);
                sm_manager_->get_bpm()->unpin_page({fh->GetFd(), item->GetRid().page_no}, true);
            }
            fh->insert_record(item->GetRid(), item->GetRecord().data);
        }
        else if(item->GetWriteType() == WType::UPDATE_TUPLE){
            auto fh = sm_manager_->fhs_.at(item->GetTableName()).get();
            // 恢复更改的索引
            auto tab_ = sm_manager_->db_.get_table(item->GetTableName()); 
            // 获取插入元素的record, 注意这里item的record存放的是原来的record
            auto Tuple = fh->get_record(item->GetRid(), nullptr);
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(item->GetTableName(), index.cols)).get();
                char* del_key = new char[index.col_tot_len];
                char* put_key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    // 现在存放的record，删掉
                    memcpy(del_key + offset, Tuple->data + index.cols[i].offset, index.cols[i].len);
                    // 之前存放的record，添加
                    memcpy(put_key + offset, item->GetRecord().data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(del_key, txn);
                ih->insert_entry(put_key, item->GetRid(), txn);
            }
            if(enable_logging){
                //写Update
                UpdateLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn(), *Tuple.get(), item->GetRecord(), item->GetRid(), item->GetTableName());
                lsn_t lsn = log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(lsn);
                Page* page = sm_manager_->get_bpm()->fetch_page({fh->GetFd(), item->GetRid().page_no});
                page->set_page_lsn(lsn);
                sm_manager_->get_bpm()->unpin_page({fh->GetFd(), item->GetRid().page_no}, true);
            }

            // TODO: 这里的nullptr后续需要替换
            fh->update_record(item->GetRid(), item->GetRecord().data, nullptr);
        }
        else assert(0);
        write_set->pop_back();
        delete item;
    }
    write_set->clear();

    // Release all the locks.
    ReleaseLocks(txn);
    txn->get_lock_set()->clear();
    
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    if(enable_logging){
        //写Abort日志
        AbortLogRecord record(txn->get_transaction_id(), txn->get_prev_lsn());
        lsn_t lsn = log_manager->add_log_to_buffer(&record);
        txn->set_prev_lsn(lsn);
        log_manager->ForceFlush(lsn);
    }

    txn->set_state(TransactionState::ABORTED);
    // Remove txn from txn_map
    std::unique_lock<std::mutex> l(latch_);
    txn_map.erase(txn->get_transaction_id());

    // delete txn;
    // txn = nullptr;
    return ;
}