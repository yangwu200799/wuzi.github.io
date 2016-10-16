---
layout: post
title:  "redis跳表实现"
date:   2016-10-16 14:36:23
category: blog
description: 项目中用到的跳表，将redis中的跳表代码抽出来，封装成类
---
#redis跳表抽取与实现
跳表的结构大家基本都有所了解，是一种随机化的数据结构，基于并联的链表。与红黑树等平衡数据结构的查找时间复杂度相同，都是O(log n)，除此之外，跳表的插入，删除时间复杂度均为O(log n)。网上有很多关于跳表原理的讲解的文章，这里只是作为平时做项目的一个总结，项目中用到了跳表，使用的是将redis中的跳表从redis源码中抽取出来的方式，并且稍加修改，封装成类。

##项目中使用跳表的场景
#### 为什么不能使用stl

项目中需要一个能够排序的数据结构，stl的map本身是红黑树实现，key有排序的功能，如果想查找某一个范围内的数据，可以使用lower_bound、upper_bound来进行范围查找。但是这种方式有个缺点，就是如果需要获取满足某个范围的数据总数，那只能通过迭代器逐个遍历获取，当数据量非常大的时候，这样做会比较耗时。如果一个在线服务，每个请求都需要如此来获取数据数目，无法满足快速响应用户的需求。

#### 跳表

跳表本身有排序的功能，如果我们想存储字符串的数据，并且希望能够按照某一个属性（整型）来排序，这样的跳表如何实现呢。对于这种需求，大家如果使用过redis，使用过redis中的sort set就会立马想到，这不就是sort set中的member和score么，member就是实际要存储的字符串数据，而score就是字符串具有的分值属性，在跳表中能够按照score排序。为了快速实现业务需求，我们将redis中的跳表代码抽取出来，封装成类，如下

***
####zskiplist.h
```
#ifndef __ZSKIPLIST_H_
#define __ZSKIPLIST_H_

#include <string>
#include <vector>
#include "tbb/concurrent_hash_map.h"
#include "com_log.h"

namespace trace {
             
#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25   /* Skiplist P = 1/4 */
             
/* Struct to hold a inclusive/exclusive range spec by score comparison. */
struct zrangespec {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
};           
             
struct zskiplistNode {
    std::string *obj;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned int span;
    } level[];
};

             
struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
};           
             
zskiplistNode *zslCreateNode(int level, double score, std::string *obj);
zskiplist *zslCreate(void);
void zslFreeNode(zskiplistNode *node);
void zslFree(zskiplist *zsl);
int zslRandomLevel(void);
zskiplistNode *zslInsert(zskiplist *zsl, double score, std::string *obj);
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update);
int zslDelete(zskiplist *zsl, double score, const std::string *obj);
int zslIsInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec range);
unsigned long zslDeleteRangeByScore(
        zskiplist *zsl,
        zrangespec range,
        tbb::concurrent_hash_map<std::string, int64_t>& dict);
unsigned long zslDeleteRangeByRank(
        zskiplist *zsl,
        unsigned int start,
        unsigned int end,
        tbb::concurrent_hash_map<std::string, int64_t>& dict);
unsigned long zslGetRank(zskiplist *zsl, double score, const std::string* obj);
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);
  
/**          
 * @brief skip list 类封装 非线程安全
 */          
class SkipList {
public:      
    SkipList();
             
    ~SkipList();
             
    /** 添加一对member和score 如果member存在则更新score */
    void add(const std::string& member, int64_t score);
             
    /** 根据member 删除记录 */
    void remove_by_member(const std::string& member);
             
    /** 根据score范围 删除记录 */
    void remove_by_score(int64_t min, int64_t max);
             
    /** 根据rank范围 删除记录 */
    void remove_by_rank(uint32_t start, uint32_t stop);
             
    /** 获取skiplist长度 */
    unsigned long get_length();
             
    /** 根据score和member获取rank */
    unsigned long get_rank(const std::string& member, int64_t score);
             
    /** 根据score区间 获取数据 */
    int range_by_score(
            int64_t min,
            bool minex,
            int64_t max,
            bool maxex,
            size_t start,
            size_t end,
            bool reverse,
            bool withscores,
                        uint32_t& total,
            std::vector<std::string>& members,
            std::vector<int64_t>& scores);
             
    /** 清除skiplist数据 */
    void clear(); 
             
private:     
    zskiplist* _zsl;
             
    /** hash表 记录member=>score */
    tbb::concurrent_hash_map<std::string, int64_t> _dict;
};           
             
}            
             
#endif           
```           
***
####zskiplist.cpp
```
/**
* @file zskiplist.cpp
* @author wuzi(yangwu@baidu.com)
* @date 2016-07-22 18:52
* @brief 
*
**/

#include <math.h>
#include <stdlib.h>
#include <assert.h>
#include "zskiplist.h"
#include "kmp.h"

namespace trace {

static int zslValueGteMin(double value, zrangespec *spec);
static int zslValueLteMax(double value, zrangespec *spec);
            
zskiplistNode *zslCreateNode(int level, double score, std::string *obj) {
skiplistNode::zskiplistLevel));istNode*)malloc(sizeof(*zn)+level*sizeof(struct z--More--(3%)
    zn->score = score;
    zn->obj = obj;
    return zn;
}           
            
zskiplist *zslCreate(void) {
    int j;  
    zskiplist *zsl;
            
    zsl = (zskiplist*)malloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }       
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}           
            
void zslFreeNode(zskiplistNode *node) {
    //decrRefCount(node->obj);
    if (node->obj != NULL) {
        delete node->obj;
        node->obj = NULL;
    }       
    if (node != NULL) {
        free(node);
    }       
}           
            
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;
            
    if (zsl->header != NULL) {
        free(zsl->header);
    }       
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }       
            
    if (zsl != NULL) {
        free(zsl);
    } 
    }           
            
int zslRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}            
             
zskiplistNode *zslInsert(zskiplist *zsl, double score, std::string *obj) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                (x->level[i].forward->obj)->compare(*obj) < 0))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }    
        update[i] = x;
    }        
    /* we assume the key is not already inside, since we allow duplicated
     * scores, and the re-insertion of score and redis object should never
     * happen since the caller of zslInsert() should test in the hash table
     * if the element is already inside or not. */
    level = zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
                    }    
        zsl->level = level;
    }        
    x = zslCreateNode(level,score,obj);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;
             
        /* update span covered by update[i] as x is inserted here */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }        
             
    /* increment span for untouched levels */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }        
             
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else     
        zsl->tail = x;
    zsl->length++;
    return x;
}            
             
/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;   
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }    
            }        
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else { 
        zsl->tail = x->backward;
    }        
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    zsl->length--;
}            
             
/* Delete an element with matching score/object from the skiplist. */
int zslDelete(zskiplist *zsl, double score, const std::string *obj) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;   
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                (x->level[i].forward->obj)->compare(*obj) < 0)))
            x = x->level[i].forward;
        update[i] = x;
    }        
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && score == x->score && *(x->obj) == *obj) {
        zslDeleteNode(zsl, x, update);
        zslFreeNode(x);
        return 1;
    } else { 
        return 0; /* not found */
    }        
    return 0; /* not found */
}            
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}            
             
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}            
             
/* Returns if there is a part of the zset is in range. */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
             
    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}            
             
/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec range) {
    zskiplistNode *x;
    int i;   
             
    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,&range)) return NULL;
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
    /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,&range))
                x = x->level[i].forward;
    }        
             
    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    assert(x != NULL);
             
    /* Check if score <= max. */
    if (!zslValueLteMax(x->score,&range)) return NULL;
    return x;
}            
             
/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec range) {
    zskiplistNode *x;
    int i;   
             
    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,&range)) return NULL;
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,&range))
                x = x->level[i].forward;
    }        
             
    /* This is an inner range, so this node cannot be NULL. */
    assert(x != NULL);
             
    /* Check if score >= min. */
    if (!zslValueGteMin(x->score,&range)) return NULL;
        return x;
}            
             
/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
unsigned long zslDeleteRangeByScore(
        zskiplist *zsl,
        zrangespec range,
        tbb::concurrent_hash_map<std::string, int64_t>& dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;   
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range.minex ?
            x->level[i].forward->score <= range.min :
            x->level[i].forward->score < range.min))
                x = x->level[i].forward;
        update[i] = x;
    }        
             
    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;
             
    /* Delete nodes while in range. */
    while (x && (range.maxex ? x->score < range.max : x->score <= range.max)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
             
        // 删除dict中记录
        dict.erase(*(x->obj));
             
        zslFreeNode(x);
        removed++;
                x = next;
    }        
    return removed;
}            
             
/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
unsigned long zslDeleteRangeByRank(
        zskiplist *zsl,
        unsigned int start,
        unsigned int end,
        tbb::concurrent_hash_map<std::string, int64_t>& dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;   
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }    
        update[i] = x;
    }        
             
    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
             
        // 删除dict中记录
        dict.erase(*(x->obj));
             
        zslFreeNode(x);
        removed++;
        traversed++;
                x = next;
    }        
    return removed;
}            
             
/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
    zskiplistNode *x;ank(zskiplist *zsl, double score, const std::string* obj) {--More--(56%)
    unsigned long rank = 0;
    int i;   
             
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                (x->level[i].forward->obj)->compare(*obj) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }    
             
        /* x might be equal to zsl->header, so test if obj is non-NULL */
        if (x->obj && *(x->obj) == *obj) {
            return rank;
        }    
    }        
    return 0;
}            
             
/* Finds an element by its rank. The rank argument needs to be 1-based. */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;   
        x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {    
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }    
        if (traversed == rank) {
            return x;
        }    
    }        
    return NULL;
}            
             
SkipList::SkipList() {
    _zsl = zslCreate();
}            
             
SkipList::~SkipList() {
    zslFree(_zsl); 
}            
             
void SkipList::add(const std::string& member, int64_t score) {
    // 先查member是否存在
    tbb::concurrent_hash_map<std::string, int64_t>::const_accessor const_acsor;
             
    // 存在 则删除跳表中老的member 插入新的 然后更新dict中member对应的score
    if (_dict.find(const_acsor, member)) {
        int64_t cur_score = const_acsor->second; 
        const_acsor.release();
             
        zslDelete(_zsl, (double)cur_score, &member); 
        std::string* m = new std::string(member);
        zslInsert(_zsl, (double)score, m);
             
        tbb::concurrent_hash_map<std::string, int64_t>::accessor acsor;
        _dict.insert(acsor, member); 
                acsor->second = score;
        return;
    }        
             
    // 不存在 作为新的member score先加入跳表 再加入dict
    std::string* m = new std::string(member);
    zslInsert(_zsl, (double)score, m); 
             
    tbb::concurrent_hash_map<std::string, int64_t>::accessor acsor;
    _dict.insert(acsor, member);
    acsor->second = score;
}            
             
void SkipList::remove_by_member(const std::string& member) {
    // 找到score 
    tbb::concurrent_hash_map<std::string, int64_t>::const_accessor const_acsor;
    if (!_dict.find(const_acsor, member)) {
r.c_str());  G_LOG("[SkipList][%s]dict not found member:%s", __FUNCTION__, membe--More--(70%)
        return;
    }        
             
    int64_t score = const_acsor->second;
    const_acsor.release();
             
    // 删除skiplist中member score记录
    zslDelete(_zsl, (double)score, &member);
             
    // 删除dict中member score记录
    //tbb::concurrent_hash_map<std::string, int64_t>::accessor acsor;
    //acsor->first = member;
    //_dict.erase(acsor);
    _dict.erase(member);
}            
             
void SkipList::remove_by_score(int64_t min, int64_t max) {
    zrangespec range;
    range.min = min;
        range.minex = 0;
    range.max = max;
    range.maxex = 0;
    unsigned long removed_size = zslDeleteRangeByScore(_zsl, range, _dict);
removed_size);("[SkipList][%s]removed success, removed_size:%lu", __FUNCTION__, --More--(74%)
}            
             
void SkipList::remove_by_rank(uint32_t start, uint32_t end) {
    unsigned long removed_size = zslDeleteRangeByRank(_zsl, start, end, _dict);
removed_size);("[SkipList][%s]removed success, removed_size:%lu", __FUNCTION__, --More--(75%)
}            
             
unsigned long SkipList::get_length() {
    return _zsl->length;
}            
             
unsigned long SkipList::get_rank(const std::string& member, int64_t score) {
    return zslGetRank(_zsl, (double)score, &member);
}            
             
int SkipList::range_by_score(
        int64_t min,
        bool minex,
        int64_t max,
        bool maxex,
        size_t start,
        size_t end,
        bool reverse,
        bool withscores,
        uint32_t& total,
        std::vector<std::string>& members,
        std::vector<int64_t>& scores) {
             
    zskiplistNode* ln = NULL; 
    zrangespec range;
    range.min = min;
    // minex=0是包括
        if (minex) {
        range.minex = 0;
    } else { 
        range.minex = 1;
    }        
    range.max = max;
    if (maxex) {
        range.maxex = 0;
    } else { 
        range.maxex = 1;
    }        
             
    unsigned long rank_first = 0;
    unsigned long rank_end = 0;
    // score由大到小排序 
    if (reverse) {
        ln = zslLastInRange(_zsl, range);
        if (ln == NULL) {
            return 0;
        }    
             
        rank_first = zslGetRank(_zsl, ln->score, ln->obj); 
        zskiplistNode* ln_end = zslFirstInRange(_zsl, range);
        rank_end = zslGetRank(_zsl, ln_end->score, ln_end->obj);
        total = rank_first - rank_end + 1;
    } else { 
        ln = zslFirstInRange(_zsl, range);
        if (ln == NULL) {
            return 0;
        }    
             
        rank_first = zslGetRank(_zsl, ln->score, ln->obj); 
        zskiplistNode* ln_end = zslLastInRange(_zsl, range);
        rank_end = zslGetRank(_zsl, ln_end->score, ln_end->obj);
        total = rank_end - rank_first + 1;
    }        
        // start位置的rank
    unsigned long rank_start = 0;
             
    if (reverse) {
        rank_start = rank_first - (unsigned long)start;
    } else { 
        rank_start = rank_first + (unsigned long)start;
    }        
             
    // 跳到start位置 
    ln = zslGetElementByRank(_zsl, rank_start);
             
    if (ln == NULL) {
        return 0;
    }        
             
    size_t idx = start;
    // 从start开始找到end
    while (ln) {
        if (idx >= end) {
            break;
        }    
             
        if (reverse) {
            if (!zslValueGteMin(ln->score, &range)) {
                break;
            }
        } else {
            if (!zslValueLteMax(ln->score, &range)) {
                break;
            }
        }    
             
        if (idx >= start && idx < end) {
            members.push_back(*(ln->obj));
            if (withscores) {
                scores.push_back(static_cast<int64_t>(ln->score));
                            }
        }    
             
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }    
        idx++;
    }        
             
    return 0;
}     
```
***

其中用到了tbb的concurrent_hash_map，这个在下一篇会有所讲解


