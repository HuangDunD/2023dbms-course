/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    if(file_handle_->file_hdr_.num_pages <= 1) {
        rid_.page_no = -1;
        rid_.slot_no = -1;
        return;
    }
    int page_no = 1;
    int slot_no = -1;

    RmPageHandle page_handle;
    do
    {
        page_handle = file_handle_->fetch_page_handle(page_no);
        slot_no = Bitmap::first_bit(1, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page);
        if(slot_no != file_handle_->file_hdr_.num_records_per_page){
            // find record
            break;
        }
        // 当前不存在可用记录，可以unpin
        file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, page_no}, false);
        page_no++;
    }while (page_no < file_handle_->file_hdr_.num_pages);
    
    if(page_no >= file_handle_->file_hdr_.num_pages){
        rid_.page_no = -1;
        rid_.slot_no = -1;
        return;
    }

    rid_.page_no = page_no;
    rid_.slot_no = slot_no;
    cur_page_hanle_ = page_handle;

    return;
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    int page_no = rid_.page_no;
    int slot_no = rid_.slot_no;

    // RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
    // slot_no = Bitmap::next_bit(1, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, slot_no);
    // file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, page_no}, false);

    slot_no = Bitmap::next_bit(1, cur_page_hanle_.bitmap, file_handle_->file_hdr_.num_records_per_page, slot_no);
    if(slot_no != file_handle_->file_hdr_.num_records_per_page){
        // find record
        rid_.page_no = page_no;
        rid_.slot_no = slot_no;
        return;
    }

    // 进到下一个page cur_page_hanle_可以释放
    file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, page_no}, false);

    RmPageHandle page_handle;
    page_no++;
    while (page_no < file_handle_->file_hdr_.num_pages){
        page_handle = file_handle_->fetch_page_handle(page_no);
        slot_no = Bitmap::first_bit(1, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page);
        if(slot_no != file_handle_->file_hdr_.num_records_per_page){
            // find record
            break;
        }
        // unpin
        file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, page_no}, false);
        page_no++;
    }
    if(page_no >= file_handle_->file_hdr_.num_pages){
        rid_.page_no = -1;
        rid_.slot_no = -1;
        return;
    }
    
    rid_.page_no = page_no;
    rid_.slot_no = slot_no;
    cur_page_hanle_ = page_handle;
    return;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    // int page_no = rid_.page_no;
    // int slot_no = rid_.slot_no;
    // RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
    // slot_no = Bitmap::next_bit(1, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, slot_no);
    // if(slot_no != file_handle_->file_hdr_.num_records_per_page){
    //     // find record
    //     return false;
    // }
    // page_no++;
    // while (page_no < file_handle_->file_hdr_.num_pages){
    //     RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
    //     slot_no = Bitmap::first_bit(1, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page);
    //     if(slot_no != file_handle_->file_hdr_.num_records_per_page){
    //         // find record
    //         return false;
    //     }
    //     page_no++;
    // }
    // return true;

    return rid_.page_no < 0 || rid_.slot_no < 0;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}