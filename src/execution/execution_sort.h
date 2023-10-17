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

#include <algorithm>

// 使用对象而不是函数作为sort函数的比较器
class CompareObj
{
    std::vector<ColMeta> sort_cols;  // 排序的字段
    std::vector<bool> is_desc_arr;
public:
    CompareObj(std::vector<ColMeta> sort_cols,std::vector<bool> is_desc_arr): 
        sort_cols(sort_cols),is_desc_arr(is_desc_arr){}

    // 两个参数必须是const 引用类型, 该函数的意思是, t1是否可以排在t2前面
    bool operator()(const std::unique_ptr<RmRecord>& t1, const std::unique_ptr<RmRecord>& t2)
    {
        // operator() basically tells us whether the passed “first” argument 
        // should be placed before the passed “second” argument or not. 
        return nextComp(t1,t2,0);
    }


    inline bool nextComp(const std::unique_ptr<RmRecord>& t1, const std::unique_ptr<RmRecord>& t2, std::size_t col_idx)
    {
        // 判断第 i 个 column
        ColMeta attr = sort_cols[col_idx];

        switch (attr.type)
        {
        case TYPE_INT:
        {   int val1 = *(int*)(t1->data+attr.offset);
            int val2 = *(int*)(t2->data+attr.offset);
                
            if(val1 == val2)
            {
                if(col_idx < sort_cols.size()-1)  // 如果两数相等并且后面还有条件
                    return nextComp(t1,t2,col_idx+1);   // 递归比较
                else
                    return false;  // sort函数要求严格弱序, 两数相等必须返回false
            }
            else
                return is_desc_arr[col_idx]?(val1 >= val2):(val1 < val2);
        }
        case TYPE_FLOAT:
        {   float val1 = *(float*)(t1->data+attr.offset);
            float val2 = *(float*)(t2->data+attr.offset);
            if(val1 == val2)
            {
                if(col_idx < sort_cols.size()-1)  // 如果两数相等并且后面还有条件
                    return nextComp(t1,t2,col_idx+1);   // 递归比较
                else
                    return false;  // sort函数要求严格弱序, 两数相等必须返回false
            }
            else
                return is_desc_arr[col_idx]?(val1 >= val2):(val1 < val2);
        }
        case TYPE_STRING:
        {   
            std::string val1 = std::string(t1->data+attr.offset,attr.len);
            std::string val2 = std::string(t2->data+attr.offset,attr.len);
            if(val1 == val2)
            {
                if(col_idx < sort_cols.size()-1)  // 如果两数相等并且后面还有条件
                    return nextComp(t1,t2,col_idx+1);   // 递归比较
                else
                    return false;  // sort函数要求严格弱序, 两数相等必须返回false
            }
            else
                return is_desc_arr[col_idx]?(val1 >= val2):(val1 < val2);
        }
        case TYPE_BIGINT:
            assert(0); //bigint 类型怎么比较?
            break;
        case TYPE_DATETIME:
            assert(0); //datatime 类型怎么比较?
            break;
        default:
            printf("invalid data type\n");
            assert(0);
            break;
        }
        return false;
    }
};


class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;  // 所有字段
    std::vector<ColMeta> sort_cols;  // 排序的字段

    size_t tuple_num;
    std::vector<bool> is_desc_arr; 
    std::unique_ptr<RmRecord> current_tuple;

    bool isend;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples;
    std::size_t curr_tup_idx;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, 
                 std::vector<ColMeta> sel_cols, 
                 std::vector<bool> is_desc) :  is_desc_arr(is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();


        // sort_cols(sel_cols);  // 不能直接用sel_cols
        // 因为下面传过来的有可能是连接后的元组, 其attr的offset会改变
        for(auto sel_col:sel_cols)
        {
            for(auto col:cols_)
            {
                if(sel_col.tab_name == col.tab_name && sel_col.name == col.name)
                    sort_cols.push_back(col);
            }
        }
        tuple_num = 0;
        isend = true;
        curr_tup_idx = 0;
    }

    void beginTuple() override {
        for(prev_->beginTuple();!prev_->is_end();prev_->nextTuple())
            sorted_tuples.push_back(std::move(prev_->Next()));  // 把元组加进去

        tuple_num = sorted_tuples.size();
        if(tuple_num != 0) // 检查一下有没有空
            isend = false;
        CompareObj cmp(sort_cols, is_desc_arr);
        std::sort(sorted_tuples.begin(), sorted_tuples.end(), cmp);         
    }

    bool is_end() const override { return isend; };

    void nextTuple() override {
        curr_tup_idx++;
        if(curr_tup_idx >= tuple_num)
            isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        auto& ref_ptr = sorted_tuples[curr_tup_idx];
        return std::make_unique<RmRecord>(ref_ptr->size, ref_ptr->data);
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const override{
        return cols_;
    }
};