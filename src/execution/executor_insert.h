/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "common/tools.h"


class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    std::string load_file_name_;     // load的文件名
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    std::vector<std::string> index_names_; 

    FILE* load_file_ = nullptr;
    char* buffer[2];
    char* values_buf_;
    size_t buffer_remain_[2] = {0, 0};
    size_t cur_pos_ = 0;
    bool cur_buffer_idx_ = 1;
    std::vector<ColMeta> all_cols;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, std::string load_file_name, Context *context) : values_(std::move(values)){
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        tab_name_ = tab_name;
        
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;

        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            index_names_.push_back( sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols) ) ;
        }

        if (!load_file_name.empty()) {    
            load_file_name_ = load_file_name;
            all_cols = sm_manager_->db_.get_table(tab_name_).cols;
            int maxColWidth = 0;
            for (auto &col : all_cols) 
                maxColWidth = (maxColWidth > col.len+1) ? maxColWidth : col.len+1;
            values_buf_ = (char*)malloc(maxColWidth * sizeof(char));
            buffer[0] = (char*)malloc(4096 * sizeof(char));
            buffer[1] = (char*)malloc(4096 * sizeof(char));
            load_file_ = fopen(load_file_name_.c_str(), "r");
            assert(load_file_ != nullptr);
            fill_buffer();
            cur_buffer_idx_ = 1 - cur_buffer_idx_;
            fill_buffer();
            while(getchar_from_buffer() != '\n'); // 跳过第一行
            values_.resize(all_cols.size());
        } else if (values_.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
    };
    ~InsertExecutor() {
        if ( !load_file_name_.empty()) {
            fclose(load_file_);
            free(values_buf_);
            free(buffer[0]);
            free(buffer[1]);
        }
    }

    inline bool fill_buffer(){
        buffer_remain_[1 - cur_buffer_idx_] = fread(buffer[1 - cur_buffer_idx_], 1, 4096, load_file_);
        return buffer_remain_[1 - cur_buffer_idx_];
    }

    inline char getchar_from_buffer(){
        if(cur_pos_ == 4096){
            cur_pos_ = 0;
            cur_buffer_idx_ = 1 - cur_buffer_idx_;
            fill_buffer();
        }
        if(cur_pos_ == buffer_remain_[cur_buffer_idx_])
            return EOF;
        return buffer[cur_buffer_idx_][cur_pos_++];
    }

    bool get_values(){
        size_t colNum = all_cols.size(), col = 0, p;
        for (char ch = getchar_from_buffer(); ch != EOF && col < colNum; ch = getchar_from_buffer(), ++col) {
            for (p = 0; (ch != ',' && ch != '\n' && ch != EOF) || (values_buf_[p] = '\0'); ch = getchar_from_buffer())
                values_buf_[p++] = ch;
            values_[col].setData(all_cols[col].type, all_cols[col].len, values_buf_, p);
            if(ch == '\n' || ch == EOF) {
                ++col;
                break;
            }
        }
        if (col == 0)return false;
        else if(col == colNum) return true;
        else throw InvalidValueCountError();
    }

    std::unique_ptr<RmRecord> Next() override {
        // for(auto& values: values_){
            // Make record buffer
        while(!load_file_ || get_values()){
        if(values_.size() != tab_.cols.size()){
            throw InvalidValueCountError();
        };
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];

            val.convert_value_to_col_type_and_fill(col);

            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }

            memcpy(rec.data + col.offset, val.raw.data, col.len);
        }
        // 判断是否唯一索引上的元素是否重复
        std::vector<char*> index_key;
        
        // if(tab_.indexes.size() == 0){
            // // 没有索引，即没有间隙，直接上X锁
            // context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
        // }

        if(!load_file_){
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
        }

        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            // auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            auto ih = sm_manager_->ihs_.at(index_names_[i]).get();
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            index_key.push_back(key);
            std::vector<Rid> tmp_val;
            // if(values_.size() <= 1){
            if(!load_file_){
                // 优化：只有在指令不为load的时候，才需要进行校验
                if(ih->get_value(key, &tmp_val, nullptr) == true){
                    // 已经存在
                    // print header into file
                    AppendToOutputFile("failure\n");
                    std::cout << "insert faliure." << std::endl;
                    return nullptr;
                }

                // Iid insert_upper_iid = ih->upper_bound(key);
                // Rid gap_rid = ih->get_rid(insert_upper_iid);
                // context_->lock_mgr_->try_lock_in_gap(context_->txn_, gap_rid, ih->GetFd());
            }
        }
        
        // Insert into record file
        if(!load_file_){
            rid_ = fh_->insert_record(rec.data, context_);
        }
        else{
            // 这里针对load file做了一个优化的insert load record
            rid_ = fh_->insert_load_record(rec.data, context_);
        }

        if(!load_file_){
            // 只有在指令不为load的时候，才需要上锁并且添加写集
            // context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd());
            // 将写入数据添加到写集
            if(context_->txn_ != nullptr){
                WriteRecord* write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
                context_->txn_->append_write_record(write_record);
            }
        }

        if(!load_file_ && enable_logging){
            //写Insert
            InsertLogRecord record(context_->txn_->get_transaction_id(), context_->txn_->get_prev_lsn(), rec, rid_, tab_name_);
            lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&record);
            context_->txn_->set_prev_lsn(lsn);
            Page* page = sm_manager_->get_bpm()->fetch_page({fh_->GetFd(), rid_.page_no});
            page->set_page_lsn(lsn);
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, true);
        }
        
        // Insert into index
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            // auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(index_names_[i]).get();
            // auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            // char* key = new char[index.col_tot_len];
            // int offset = 0;
            // for(size_t i = 0; i < (size_t)index.col_num; ++i) {
            //     memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
            //     offset += index.cols[i].len;
            // }
            if(!load_file_){
                ih->insert_entry(index_key[i], rid_, context_->txn_);
            }
            else{
                ih->insert_entry_for_load(index_key[i], rid_, context_->txn_);
            }

            delete[] index_key[i];
        }
        if(!load_file_) return nullptr; // 单条insert 一次退出
        }
        // load file进行的insert executor算子结束, unpin 最后一个page
        if(fh_->page_handle_.page != nullptr){
            sm_manager_->get_bpm()->unpin_page(fh_->page_handle_.page->get_page_id(), true);
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto ih = sm_manager_->ihs_.at(index_names_[i]).get();
                ih->unpin_n_page();
            }
            auto file_hdr = fh_->get_file_hdr();
            sm_manager_->disk_manager_->write_page(fh_->GetFd(), RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
            // test del wlach
            // fh_->page_handle_.page->WUnlatch();
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};