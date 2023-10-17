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
#include "record/rm_scan.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同
    std::vector<ColMeta> lhs_cols_meta;       // condition列的meta

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator
    RmScan* rm_scan_;

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;

        for(auto &cond: conds_){
            lhs_cols_meta.push_back(*tab.get_col(cond.lhs_col.col_name));
        }
    }

    template<typename T, typename U>
    bool compare(const T& a, const U& b, CompOp op) {
        switch (op) {
            case OP_EQ:
                return a == b;
            case OP_NE:
                return a != b;
            case OP_LT:
                return a < b;
            case OP_GT:
                return a > b;
            case OP_LE:
                return a <= b;
            case OP_GE:
                return a >= b;
            default:
                std::cout << "Invalid comparison operator" << std::endl;
                assert(0);
                return false;
        }
    }

    bool check_cond(){
        // RmRecord* Tuple = get_record();
        // std::unique_ptr<RmRecord> Tuple = Next();
        // TabMeta &lhs_tab = sm_manager_->db_.get_table(tab_name_);
        // for(auto &cond: conds_){

        const char* data = get_record();

        for(size_t i=0; i<conds_.size(); i++){
            auto& cond = conds_[i];
            auto& lhs_col = lhs_cols_meta[i];
            if(cond.is_rhs_val){
                const char *lhs_data = data + lhs_col.offset;
                bool res;
                if(lhs_col.type != cond.rhs_val.type){
                    if (lhs_col.type == TYPE_INT && cond.rhs_val.type == TYPE_BIGINT){
                        res = compare(*(long long *)lhs_data, cond.rhs_val.bigint_val, cond.op);
                    } else {
                        double lvalue, rvalue;
                        if (lhs_col.type == TYPE_BIGINT) {
                            lvalue = (double)*(long long *)lhs_data;
                        } else if (lhs_col.type == TYPE_FLOAT) {
                            lvalue = (double)*(float *)lhs_data;
                        }
                        
                        if (cond.rhs_val.type == TYPE_BIGINT) {
                            rvalue = (double)cond.rhs_val.bigint_val;
                        } else if (cond.rhs_val.type == TYPE_FLOAT) {
                            rvalue = (double)cond.rhs_val.float_val;
                        }
                        res = compare(lvalue, rvalue, cond.op);
                    }
                } else {
                    switch(lhs_col.type){
                        case TYPE_INT:
                            res = compare(*(int*)lhs_data, cond.rhs_val.int_val, cond.op);
                            break;
                        case TYPE_BIGINT:
                            res = compare(*(long long*)lhs_data, cond.rhs_val.bigint_val, cond.op);
                            break;
                        case TYPE_FLOAT:
                            res = compare(*(float*)lhs_data, cond.rhs_val.float_val, cond.op);
                            break;
                        case TYPE_STRING:
                        case TYPE_DATETIME:
                            res = compare(std::string(data + lhs_col.offset, lhs_col.len), cond.rhs_val.getString(), cond.op);
                            break;                  
                    }
                }

                if (!res){
                    // delete Tuple;
                    // Tuple = nullptr;
                    return false;
                }
            } 
            else assert(0);
        }
        // delete Tuple;
        // Tuple = nullptr;
        return true;
    }

    void beginTuple() override {  
        // 顺序扫描，给表上读锁
        context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        scan_ = std::make_unique<RmScan>(fh_);
        rm_scan_ = dynamic_cast<RmScan* >(scan_.get());

        for( ; !is_end() && !check_cond(); scan_->next());
    }

    void nextTuple() override {
        for(scan_->next(); !is_end() && !check_cond(); scan_->next());
        return;
    }

    std::unique_ptr<RmRecord> Next() override {
        // rid_ = scan_->rid();
        // return fh_->get_record(rid_, nullptr);

        // scan_内的rid_所在的page是还没有unpin的，所以无需fetch
        
        rid_ = scan_->rid();
        // RmScan* rm_scan_ = dynamic_cast<RmScan* >(scan_.get());
        int slot_no = rm_scan_->rid().slot_no;
        RmPageHandle page_handle = rm_scan_->get_cur_page_hanle_();
        // 由于有表的S锁，page latch是不必要的
        // page_handle.page->Rlatch();
        std::unique_ptr<RmRecord> record_ptr = std::make_unique<RmRecord>(page_handle.file_hdr->record_size, page_handle.get_slot(slot_no));
        // page_handle.page->RUnlatch();

        return record_ptr;
    }

    char* get_record() {

        rid_ = scan_->rid();
        int slot_no = rm_scan_->rid().slot_no;
        RmPageHandle page_handle = rm_scan_->get_cur_page_hanle_();

        // 由于有表的S锁，page latch是不必要的
        // page_handle.page->Rlatch();
        // RmRecord* record_ptr = new RmRecord(page_handle.file_hdr->record_size, page_handle.get_slot(slot_no));
        // page_handle.page->RUnlatch();
        
        return page_handle.get_slot(slot_no);
    }

    bool is_end() const override { return scan_->is_end();}

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override { return cols_;};

    size_t tupleLen() override { return len_;}
};