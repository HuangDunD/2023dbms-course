/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"

// 每隔50ms刷新一次
static constexpr auto LOG_TIMEOUT = std::chrono::milliseconds(30);

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {

    std::unique_lock<std::mutex> l(latch_);
    if (log_buffer_->is_full(log_record->log_tot_len_)) {
        needFlush_ = true;
        cv_.notify_one(); //let RunFlushThread wake up.
        while(log_buffer_->is_full(log_record->log_tot_len_))
            operation_cv_.wait(l, [&] {return !log_buffer_->is_full(log_record->log_tot_len_);});
    }
    log_record->lsn_ = global_lsn_.fetch_add(1);
    log_record->serialize(log_buffer_->buffer_ + log_buffer_->offset_); 
    log_buffer_->offset_ += log_record->log_tot_len_;
    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    if (flush_buffer_->offset_ == 0) return;
    // append the log
    disk_manager_->write_log(flush_buffer_->buffer_, flush_buffer_->offset_);
    flush_buffer_->offset_ = 0;
}

void LogManager::RunFlushThread() {
    flush_thread_ = new std::thread([&]{        
        while(true){
            std::unique_lock<std::mutex> l(latch_);
            //每隔LOG_TIMEOUT刷新一次或者buffer已满或者强制刷盘
            cv_.wait_for(l, LOG_TIMEOUT, [&] {return needFlush_.load();});

            if(log_buffer_->offset_ > 0){
                // swap buffer
                std::swap(log_buffer_, flush_buffer_);
                // flush_buffer_->offset_ = log_buffer_->offset_;
                // log_buffer_->offset_ = 0;
                lsn_t lsn = global_lsn_ - 1;
                l.unlock();
                // resume the append log record operation 
                operation_cv_.notify_all();
                //flush log to disk
                flush_log_to_disk();

                persist_lsn_ = lsn;
                needFlush_.store(false);
            }
        }
    });
}

void LogManager::ForceFlush(lsn_t lsn) {
    if( persist_lsn_ >= lsn) return;    
    needFlush_ = true;
    // notify flush thread to start flushing the log
    cv_.notify_one();
    while (persist_lsn_ < lsn) {}
    return;
}
