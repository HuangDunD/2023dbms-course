/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    // std::unique_lock<std::mutex> l(latch_);
    if(!free_list_.empty()){
        // 有空闲帧
        *frame_id = free_list_.front();
        free_list_.pop_front();
        // l.unlock();
        return true;
    }
    else{
        return replacer_->victim(frame_id);
    }
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    if(page->is_dirty()){
        // for logging
        auto lsn = page->get_page_lsn();
        log_manager_->ForceFlush(lsn);
        
        disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->data_, PAGE_SIZE);
    }
    // std::unique_lock<std::mutex> l(latch_);
    // page_table_.erase(page->get_page_id());
    // page_table_[new_page_id] = new_frame_id;
    // l.unlock();

    page->id_ = new_page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    page->reset_memory();
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    //Todo:
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页
    // std::cout << "fetch page :" << page_id.page_no << std::endl;

    std::unique_lock<std::mutex> l(latch_);
    Page* loc_page;
    frame_id_t frame_id;
    if(page_table_.count(page_id)){
        frame_id = page_table_[page_id];
        // lock frame
        // while(true){
        //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
        //     if( is_latch == false) break;
        //     // else std::cout << "hcy" ;
        // }
        (pagelatch_+frame_id)->lock();
        // 检查获取的页是否是期望的
        loc_page = pages_ + frame_id;
        if( !(loc_page->get_page_id() == page_id) ){
            // 不等于page_id, 首先将这个页从页表删除(延迟删除)，之后重新fetch
            page_table_.erase(page_id);
            (pagelatch_+frame_id)->unlock();
            // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
            l.unlock();
            return fetch_page(page_id);
        }
        replacer_->pin(frame_id);
        // 是期望的页，l可以释放
        l.unlock();
        loc_page->pin_count_ ++;
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    }
    else{
        if(find_victim_page(&frame_id) == false){
            throw BufferpoolFullError();
            return nullptr;
        }
        // 得到分配的帧号
        loc_page = pages_+ frame_id;
        // 对帧上锁, cas操作，0 -> 1
        // while(true){
        //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
        //     if( is_latch == false) break;
        //     // else std::cout << "hcy" ;
        // }
        (pagelatch_+frame_id)->lock();
        // 在这里更新页表
        page_table_[page_id] = frame_id;
        l.unlock();

        // 上了frame的锁, 慢慢update, 此时新旧page_id都会映射到这个帧
        update_page(loc_page, page_id, frame_id);
        // std::cout << "now read page from disk: " << page_id.page_no << std::endl;
        disk_manager_->read_page(page_id.fd, page_id.page_no, loc_page->data_, PAGE_SIZE);
        loc_page->pin_count_ ++;
        // page_table_.erase(loc_page->get_page_id());
        // 读取完毕，解锁
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    }
    return loc_page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_

    std::unique_lock<std::mutex> l(latch_);
    if(!page_table_.count(page_id)){
        // Not found page in page table
        return false;
    }
    int frame_id = page_table_[page_id];
    Page* page = pages_ + frame_id;

    // 对帧上锁, cas操作，0 -> 1
    // while(true){
    //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
    //     if( is_latch == false) break;
    //     // else std::cout << "hcy" ;
    // }
    (pagelatch_+frame_id)->lock();
    // 检查获取的页是否是期望的
    if( !(page->get_page_id() == page_id) ){
        // 不等于page_id, 首先将这个页从页表删除(延迟删除)，之后重新fetch
        page_table_.erase(page_id);
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
        return false;
    }

    int pin_cnt = page->pin_count_;
    if(pin_cnt == 0){
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
        return false;
    }
    if(--page->pin_count_ == 0){
        replacer_->unpin(page_table_[page_id]);
    }
    if(page->is_dirty_ == false) {
        page->is_dirty_ = is_dirty;
    }
    (pagelatch_+frame_id)->unlock();
    // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    // std::cout << "flush_page: " << page_id.page_no << std::endl;
    std::unique_lock<std::mutex> l(latch_);
    if(!page_table_.count(page_id)){
        // Not found page in page table
        return false;
    }
    int frame_id = page_table_[page_id];
    Page* page = pages_ + frame_id;
    // while(true){
    //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
    //     if( is_latch == false) break;
    //     // else std::cout << "hcy" ;
    // }
    (pagelatch_+frame_id)->lock();
    if( !(page->get_page_id() == page_id) ){
        // 不等于page_id, 首先将这个页从页表删除(延迟删除)
        page_table_.erase(page_id);
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
        return false;
    }
    l.unlock();
    // for logging
    auto lsn = page->get_page_lsn();
    log_manager_->ForceFlush(lsn);

    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    page->is_dirty_ = false;
    (pagelatch_+frame_id)->unlock();
    // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    // free_list_.push_back(page_table_[page_id]);
    // page_table_.erase(page->get_page_id());
    // page->pin_count_ = 0;
    // page->reset_memory();
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page

    // 分配一个可用的frame
    int frame_id;
    std::unique_lock<std::mutex> l(latch_);
    if(find_victim_page(&frame_id) == false) return nullptr;

    // 在磁盘上分配一个新的fd中的page_id
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);

    // 对帧上锁, cas操作，0 -> 1
    // while(true){
    //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
    //     if( is_latch == false) break;
    //     // else std::cout << "hcy" ;
    // }
    (pagelatch_+frame_id)->lock();

    page_table_[*page_id] = frame_id;
    replacer_->pin(frame_id);
    l.unlock();

    // std::cout << "new page :" << page_id->page_no << std::endl;

    update_page(pages_ + frame_id, *page_id, frame_id);
    // flush_page(*page_id);

    // 写回磁盘
    // for logging
    auto lsn = (pages_ + frame_id)->get_page_lsn();
    log_manager_->ForceFlush(lsn);
    disk_manager_->write_page(page_id->fd, page_id->page_no, (pages_ + frame_id)->data_, PAGE_SIZE);

    (pages_ + frame_id)->pin_count_ = 1;

    (pagelatch_+frame_id)->unlock();
    // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    
    return pages_ + frame_id;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    // std::cout << "delete page :" << page_id.page_no << std::endl;
    std::unique_lock<std::mutex> l(latch_);
    disk_manager_->deallocate_page(page_id.page_no);
    if(!page_table_.count(page_id)){
        // Not found page in page table
        return true;
    }
    frame_id_t frame_id = page_table_[page_id];
    Page* page = pages_ + frame_id;
    // while(true){
    //     bool is_latch = __sync_val_compare_and_swap(frame_latch_ + frame_id, false, true);
    //     if( is_latch == false) break;
    //     // else std::cout << "hcy" ;
    // }
    (pagelatch_+frame_id)->lock();
    if( !(page->get_page_id() == page_id) ){
        // 不等于page_id, 首先将这个页从页表删除(延迟删除)
        page_table_.erase(page_id);
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
        return true;
    }

    if(page->pin_count_ != 0){
        (pagelatch_+frame_id)->unlock();
        // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
        return false;
    }
    // disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

    page_table_.erase(page->get_page_id());
    free_list_.push_back(frame_id);
    l.unlock();

    page->id_ = {-1 , INVALID_PAGE_ID };
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    page->reset_memory();
    (pagelatch_+frame_id)->unlock();
    // assert(true == __sync_val_compare_and_swap(frame_latch_ + frame_id, true, false));
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    // std::unique_lock<std::mutex> l(latch_);
    // std::cout << "flush_all_page: " << fd << std::endl;
    for (size_t i = 0; i < pool_size_; i++) {
        // flush_page(pages_[i].get_page_id());
        if(pages_[i].get_page_id().fd == fd){
            flush_page(pages_[i].get_page_id());
        }
    }
}

/**
 * @description: 将buffer_pool中的所有的fd页删除
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::del_all_pages(int fd){
    for (size_t i = 0; i < pool_size_; i++) {
        if(pages_[i].get_page_id().fd == fd){
            delete_page(pages_[i].get_page_id());
        }
    }
}