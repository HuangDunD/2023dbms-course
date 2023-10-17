/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    int offset = 0;
    int max_lsn = INVALID_LSN;
    int read_bytes;
    while ((read_bytes = disk_manager_->read_log(buffer_.buffer_, LOG_BUFFER_SIZE, offset)) > 0) {
        buffer_.offset_ = read_bytes - 1;
        int inner_offset = 0;
        while (inner_offset <= buffer_.offset_ ) {

            if (inner_offset + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > LOG_BUFFER_SIZE) break;
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TOT_LEN);
            if (size == 0 || size + inner_offset > LOG_BUFFER_SIZE) {
                break;
            }
            
            LogRecord *record;
            LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_ + inner_offset);
            switch (type)
            {
            case LogType::begin :
                record = new BeginLogRecord();
                break;
            case LogType::ABORT :
                record = new AbortLogRecord();
                break;
            case LogType::commit :
                record = new CommitLogRecord();
                break;
            case LogType::INSERT :
                record = new InsertLogRecord();
                break;
            case LogType::UPDATE :
                record = new UpdateLogRecord();
                break;
            case LogType::DELETE :
                record = new DeleteLogRecord();
                break;
            default:
                assert(0);
                break;
            }
            record->deserialize(buffer_.buffer_ + inner_offset);
            // update max lsn
            max_lsn = std::max(max_lsn, record->lsn_);
            // remember the necessary information to retrieve log based on lsn
            lsn_mapping_[record->lsn_] = std::make_pair(offset + inner_offset, size);
            // redo the log if necessary
            RedoLog(record);
            delete record;
            inner_offset += size;
        }
        offset += inner_offset;
    }
    log_manager_->set_persist_lsn(max_lsn);
    log_manager_->set_global_lsn(max_lsn + 1);
}

void RecoveryManager::RedoLog(LogRecord *log_record) {
    // record last lsn
    active_txn_[log_record->log_tid_] = log_record->lsn_;

    switch (log_record->log_type_) {
    case LogType::commit:
    case LogType::ABORT:
        active_txn_.erase(log_record->log_tid_);

        std::cout << "Redo: ";
        log_record->format_print();
        break;
    case LogType::begin:
        active_txn_[log_record->log_tid_] = log_record->lsn_;
        break;
    case LogType::INSERT: {
        InsertLogRecord* insert_record = dynamic_cast<InsertLogRecord*>(log_record);
        std::string tab_name(insert_record->table_name_, insert_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, insert_record->rid_.page_no});

        // if this log has been persisted on disk, then we don't need to redo it
        if (page->get_page_lsn() >= insert_record->lsn_) {
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
            break;
        }

        filehandle->insert_record(insert_record->rid_, insert_record->insert_value_.data);
        page->set_page_lsn(insert_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);

        std::cout << "Redo: ";
        insert_record->format_print();
        break;
    }
    case LogType::DELETE: {
        DeleteLogRecord* delete_record = dynamic_cast<DeleteLogRecord*>(log_record);
        std::string tab_name(delete_record->table_name_, delete_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, delete_record->rid_.page_no});

        // if this log has been persisted on disk, then we don't need to redo it
        if (page->get_page_lsn() >= delete_record->lsn_) {
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
            break;
        }

        filehandle->delete_record(delete_record->rid_, nullptr);
        page->set_page_lsn(delete_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);

        std::cout << "Redo: ";
        delete_record->format_print();
        break;
    }
    case LogType::UPDATE: {
        UpdateLogRecord* update_record = dynamic_cast<UpdateLogRecord*>(log_record);
        std::string tab_name(update_record->table_name_, update_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, update_record->rid_.page_no});

        // if this log has been persisted on disk, then we don't need to redo it
        if (page->get_page_lsn() >= update_record->lsn_) {
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);
            break;
        }

        filehandle->update_record(update_record->rid_, update_record->new_value_.data, nullptr);
        page->set_page_lsn(update_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);

        std::cout << "Redo: ";
        update_record->format_print();
        break;
    }
    default:
        assert(0);
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    std::set<lsn_t> next_lsn;
    for (auto [key, lsn]: active_txn_) {
        next_lsn.insert(lsn);
    }

    while (!next_lsn.empty()) {
        auto lsn = *next_lsn.rbegin();
        // first fetch the offset and size
        auto [offset, size] = lsn_mapping_[lsn];
        disk_manager_->read_log(buffer_.buffer_, size, offset);

        LogRecord *record;
        LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_);
        switch (type)
        {
        case LogType::begin :
            record = new BeginLogRecord();
            break;
        case LogType::ABORT :
            record = new AbortLogRecord();
            break;
        case LogType::commit :
            record = new CommitLogRecord();
            break;
        case LogType::INSERT :
            record = new InsertLogRecord();
            break;
        case LogType::UPDATE :
            record = new UpdateLogRecord();
            break;
        case LogType::DELETE :
            record = new DeleteLogRecord();
            break;
        default:
            assert(0);
            break;
        }
        // deserialize the log
        record->deserialize(buffer_.buffer_);

        UndoLog(record);
        // erase current lsn and insert the previous lsn
        next_lsn.erase(lsn);
        if (record->prev_lsn_ != INVALID_LSN) {
            next_lsn.insert(record->prev_lsn_);
        }
    }
    for (auto &[txn_id, lsn]: active_txn_) {
        auto abortlog = AbortLogRecord(txn_id, lsn);
        log_manager_->add_log_to_buffer(&abortlog);
    }
}

void RecoveryManager::UndoLog(LogRecord *log_record) {
    switch (log_record->log_type_) {
    case LogType::begin:
        // do nothing
        break;
    case LogType::INSERT: {
        
        InsertLogRecord* insert_record = dynamic_cast<InsertLogRecord*>(log_record);
        std::string tab_name(insert_record->table_name_, insert_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, insert_record->rid_.page_no});

        auto log = DeleteLogRecord(insert_record->log_tid_, active_txn_[insert_record->log_tid_], insert_record->insert_value_, insert_record->rid_, tab_name);
        log_manager_->add_log_to_buffer(&log);
        filehandle->delete_record(insert_record->rid_, nullptr);
        page->set_page_lsn(insert_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);
        // update last lsn
        active_txn_[insert_record->log_tid_] = log.lsn_;

        std::cout << "Undo: ";
        insert_record->format_print();
        break;
    }
    case LogType::DELETE: {
        DeleteLogRecord* delete_record = dynamic_cast<DeleteLogRecord*>(log_record);
        std::string tab_name(delete_record->table_name_, delete_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, delete_record->rid_.page_no});

        auto log = InsertLogRecord(delete_record->log_tid_, active_txn_[delete_record->log_tid_], delete_record->delete_value_, delete_record->rid_, tab_name);
        log_manager_->add_log_to_buffer(&log);
        filehandle->insert_record(delete_record->rid_, delete_record->delete_value_.data);
        page->set_page_lsn(delete_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);
        // update last lsn
        active_txn_[delete_record->log_tid_] = log.lsn_;

        std::cout << "Undo: ";
        delete_record->format_print();
        break;
    }
    case LogType::UPDATE: {
        UpdateLogRecord* update_record = dynamic_cast<UpdateLogRecord*>(log_record);
        std::string tab_name(update_record->table_name_, update_record->table_name_size_);
        auto filehandle = sm_manager_->fhs_[tab_name].get();
        auto fd = sm_manager_->fhs_[tab_name]->GetFd();
        auto page = buffer_pool_manager_->fetch_page({fd, update_record->rid_.page_no});

        auto log = UpdateLogRecord(update_record->log_tid_, active_txn_[update_record->log_tid_], update_record->new_value_, update_record->old_value_,  update_record->rid_, tab_name);
        log_manager_->add_log_to_buffer(&log);
        filehandle->update_record(update_record->rid_, update_record->old_value_.data, nullptr);
        page->set_page_lsn(update_record->lsn_);
        buffer_pool_manager_->unpin_page(page->get_page_id(), true);
        // update last lsn
        active_txn_[update_record->log_tid_] = log.lsn_;

        std::cout << "Undo: ";
        update_record->format_print();
        break;
    }
    default:
        std::cerr <<  "Invalid Log Type";
    }
}