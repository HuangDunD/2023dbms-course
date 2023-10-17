/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

const char *help_info = "Supported SQL syntax:\n"
                   "  command ;\n"
                   "command:\n"
                   "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                   "  DROP TABLE table_name\n"
                   "  CREATE INDEX table_name (column_name)\n"
                   "  DROP INDEX table_name (column_name)\n"
                   "  INSERT INTO table_name VALUES (value [, value ...])\n"
                   "  DELETE FROM table_name [WHERE where_clause]\n"
                   "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                   "  SELECT selector FROM table_name [WHERE where_clause]\n"
                   "type:\n"
                   "  {INT | FLOAT | CHAR(n)}\n"
                   "where_clause:\n"
                   "  condition [AND condition ...]\n"
                   "condition:\n"
                   "  column op {column | value}\n"
                   "column:\n"
                   "  [table_name.]column_name\n"
                   "op:\n"
                   "  {= | <> | < | > | <= | >=}\n"
                   "selector:\n"
                   "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context){
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch(x->tag) {
            case T_CreateTable:
            {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable:
            {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case T_CreateIndex:
            {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                // 创建索引时将当前表中的所有tuple插入到B+树中
                std::unique_ptr<SeqScanExecutor> exec = std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, std::vector<Condition>(), context);
                // open index
                auto ih_ptr = sm_manager_->get_ix_manager()->open_index(x->tab_name_, x->tab_col_names_ );
                sm_manager_->ihs_.emplace(sm_manager_->get_ix_manager()->get_index_name(x->tab_name_, x->tab_col_names_), std::move(ih_ptr));
                auto index = sm_manager_->db_.get_table(x->tab_name_).get_index_meta(x->tab_col_names_);
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(x->tab_name_, x->tab_col_names_)).get();
                for(exec->beginTuple(); !exec->is_end() ; exec->nextTuple()){
                    auto rec = exec->Next(); 
                    char* key = new char[(*index).col_tot_len];
                    int offset = 0;
                    for(size_t i = 0; i < (size_t)(*index).col_num; ++i) {
                        memcpy(key + offset, rec->data + (*index).cols[i].offset, (*index).cols[i].len);
                        offset += (*index).cols[i].len;
                    }
                    ih->insert_entry(key, exec->rid(), context->txn_);
                }
                break;
            }
            case T_DropIndex:
            {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;  
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort; show indexs;语句 
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch(x->tag) {
            case T_Help:
            {
                memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_Set:{
                if (x->tab_name_ == "output_file")
                    enable_output_file = x->value_[0] - '0';
                break;
            }
            case T_ShowTable:
            {
                sm_manager_->show_tables(context);
                break;
            }
            case T_ShowIndex :
            {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_DescTable:
            {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_Transaction_begin:
            {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                break;
            }  
            case T_Transaction_commit:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->commit(context->txn_, context->log_mgr_);
                break;
            }    
            case T_Transaction_rollback:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                break;
            }    
            case T_Transaction_abort:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                break;
            }     
            default:
                throw InternalError("Unexpected field type");
                break;                        
        }

    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                            Context *context) {
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::stringstream ss;
    ss << "|";
    for(size_t i = 0; i < captions.size(); ++i) {
        ss << " " << captions[i] << " |";
    }
    ss << "\n";

    // Print records
    size_t num_rec = 0, limit_num = -1; // size_t 是无符号整数, -1即为最大值
    limit_num = executorTreeRoot->limit_num;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end() && num_rec < limit_num; executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int *)rec_buf);
            } else if (col.type == TYPE_BIGINT) {
                col_str = std::to_string(*(long long *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float *)rec_buf);
            } else if (col.type == TYPE_DATETIME) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        ss << "|";
        for(size_t i = 0; i < columns.size(); ++i) {
            ss << " " << columns[i] << " |";
        }
        ss << "\n";
        num_rec++;
    }
    AppendToOutputFile(ss.str());
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行聚合操作的select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from_with_aggregate(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                            Context *context) {
    assert(sel_cols.size()==1);  // 目前只允许select中只带有一个聚合运算的sql

    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.alias_name);  // 用别名显示
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::stringstream ss;
    ss << "|";
    for(size_t i = 0; i < captions.size(); ++i) {
        ss << " " << captions[i] << " |";
    }
    ss << "\n";

    // Print records
    size_t num_rec = 0;

    // result
    auto agg_col = executorTreeRoot->cols().at(0);
    Value res_val;
    res_val.type = agg_col.type;
    res_val.int_val = 0;
    std::string res_str;
    std::vector<std::string> columns;

    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();  // 获取一行tuple的每列，类型不同
        
        char *rec_buf = Tuple->data + agg_col.offset;
        if (agg_col.type == TYPE_INT) {
            int cur_val =  (*(int *)rec_buf);
            if (agg_col.agg_type == ast::SV_AGG_SUM) {
                res_val.int_val += cur_val;
            // } else if (agg_col.agg_type == ast::SV_AGG_COUNT) {
            //     res_val.int_val += 1;
            } else if (agg_col.agg_type == ast::SV_AGG_MAX) {
                res_val.int_val = res_val.int_val > cur_val ? res_val.int_val : cur_val;
            } else if (agg_col.agg_type == ast::SV_AGG_MIN) {
                if (num_rec == 0) {
                    res_val.int_val = cur_val;
                } else {
                    res_val.int_val = res_val.int_val < cur_val ? res_val.int_val : cur_val;
                }
            }
        } else if (agg_col.type == TYPE_BIGINT) {
            long long cur_val =  (*(long long *)rec_buf);
            if (agg_col.agg_type == ast::SV_AGG_SUM) {
                res_val.bigint_val += cur_val;
            // } else if (agg_col.agg_type == ast::SV_AGG_COUNT) {
            //     res_val.int_val += 1;
            } else if (agg_col.agg_type == ast::SV_AGG_MAX) {
                res_val.bigint_val = res_val.bigint_val > cur_val ? res_val.bigint_val : cur_val;
            } else if (agg_col.agg_type == ast::SV_AGG_MIN) {
                if (num_rec == 0) {
                    res_val.bigint_val = cur_val;
                } else {
                    res_val.bigint_val = res_val.bigint_val < cur_val ? res_val.bigint_val : cur_val;
                }
            }
        } else if (agg_col.type == TYPE_FLOAT) {
            float cur_val =  (*(float *)rec_buf);
            if (agg_col.agg_type == ast::SV_AGG_SUM) {
                res_val.float_val += cur_val;
            // } else if (agg_col.agg_type == ast::SV_AGG_COUNT) {
            //     res_val.int_val += 1;
            } else if (agg_col.agg_type == ast::SV_AGG_MAX) {
                res_val.float_val = res_val.float_val > cur_val ? res_val.float_val : cur_val;
            } else if (agg_col.agg_type == ast::SV_AGG_MIN) {
                if (num_rec == 0) {
                    res_val.float_val = cur_val;
                } else {
                    res_val.float_val = res_val.float_val < cur_val ? res_val.float_val : cur_val;
                }
            }
        } else if (agg_col.type == TYPE_STRING || agg_col.type == TYPE_DATETIME) {
            std::string cur_val = std::string((char *)rec_buf, agg_col.len);
            std::string res_str(std::move(res_val.getString()));
            if (agg_col.agg_type == ast::SV_AGG_SUM) {
                throw AggregateError();
            // } else if (agg_col.agg_type == ast::SV_AGG_COUNT) {
            //     res_val.int_val += 1;
            } else if (agg_col.agg_type == ast::SV_AGG_MAX) {
                  if (num_rec == 0) {
                    res_str = cur_val;
                    res_val.raw.SetSize(agg_col.len);
                } else {
                    res_str = res_str > cur_val ? res_str : cur_val;
                }
            } else if (agg_col.agg_type == ast::SV_AGG_MIN) {
                  if (num_rec == 0) {
                    res_str = cur_val;
                    res_val.raw.SetSize(agg_col.len);
                } else {
                    res_str = res_str < cur_val ? res_str : cur_val;
                }
            }
            memset(res_val.raw.data, 0, res_val.raw.size);
            memcpy(res_val.raw.data, res_str.c_str(), res_str.size());
        }
        if (agg_col.agg_type == ast::SV_AGG_MAX || agg_col.agg_type == ast::SV_AGG_MIN)
            num_rec = 1;
        else
            ++num_rec;
    }

    if (agg_col.agg_type == ast::SV_AGG_COUNT) {
        columns.push_back(std::to_string(num_rec));
    } else if (res_val.type == TYPE_INT) {
        columns.push_back(std::to_string(res_val.int_val));
    } else if (res_val.type == TYPE_BIGINT) {
        columns.push_back(std::to_string(res_val.bigint_val));
    } else if (res_val.type == TYPE_FLOAT) {
        columns.push_back(std::to_string(res_val.float_val));
    } else if (res_val.type == TYPE_STRING || res_val.type == TYPE_DATETIME) {
        columns.push_back(res_val.getString());
    } else {
        throw AggregateError();
    }
    
    // print record into buffer
    rec_printer.print_record(columns, context);
    // print record into file
    ss << "|";
    for(size_t i = 0; i < columns.size(); ++i) {
        ss << " " << columns[i] << " |";
    }
    ss << "\n";

    AppendToOutputFile(ss.str());
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec){
    exec->Next();
}