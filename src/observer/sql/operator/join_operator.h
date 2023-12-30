/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2021/6/10.
//

#pragma once

#include "sql/parser/parse.h"
#include "sql/operator/operator.h"
#include "rc.h"

#include <vector>

// 执行两表的join操作
class JoinOperator : public Operator
{
public:
  JoinOperator(Operator *left, Operator *right);

  ~JoinOperator();

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override ;

private:
  RC get_right_table();

  Operator *left_ = nullptr;
  Operator *right_ = nullptr;
  //bool round_done_ = true;
  JoinedTuple tuple_;
  bool is_first_ = true;//辅助笛卡尔积过程，从左元组开始算

  std::vector<CompoundRecord>::iterator rht_it_;//右表的迭代器
  std::vector<CompoundRecord> rht_;//右表，元组的数组的数组
};
