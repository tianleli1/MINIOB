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
// Created by Wangyunlai on 2021/5/14.
//

#pragma once

#include <memory>
#include <vector>

#include "common/log/log.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple_cell.h"
#include "sql/expr/expression.h"
#include "storage/record/record.h"

class Table;

class TupleCellSpec
{
public:
  TupleCellSpec() = default;
  TupleCellSpec(Expression *expr) : expression_(expr)
  {}

  ~TupleCellSpec()
  {
    if (expression_) {
      delete expression_;
      expression_ = nullptr;
    }
  }

  //重载，string类型的指针不是直接*，而是std::shared_ptr<std::string
  void set_alias(std::shared_ptr<std::string> ptr_str)
  {
    this->alias_ = ptr_str;
  }

  //因为更改了alias_的数据类型，所以要修改原函数
  void set_alias(const char *alias)
  {
    //this->alias_ = alias;
    this->alias_ = std::shared_ptr<std::string>(new std::string(alias));
  }

  std::shared_ptr<std::string> get_alias_ptr()
  {
    return alias_;
  }


  //因为更改了alias_的数据类型，所以要修改原函数
  const char *alias() const
  {
    //return alias_;
    return alias_.get()->c_str();
  }

  Expression *expression() const
  {
    return expression_;
  }

private:
  //要更改数据类型，因为设置列名时考虑多表，最终得到的数据类型是std::string。std::string类型数据的指针的类型是std::shared_ptr<std::string>
  std::shared_ptr<std::string> alias_ = nullptr;
  Expression *expression_ = nullptr;
};

class Tuple
{
public:
  Tuple() = default;
  virtual ~Tuple() = default;

  virtual int cell_num() const = 0; 
  virtual RC  cell_at(int index, TupleCell &cell) const = 0;
  virtual RC  find_cell(const Field &field, TupleCell &cell) const = 0;

  virtual RC  cell_spec_at(int index, const TupleCellSpec *&spec) const = 0;

  virtual void get_record(CompoundRecord &record) const = 0;
  virtual void set_record(CompoundRecord &record) = 0;
  virtual void set_right_record(CompoundRecord &record) = 0;
};

class RowTuple : public Tuple
{
public:
  RowTuple() = default;
  virtual ~RowTuple()
  {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_record(CompoundRecord &record) override
  {
    this->record_ = record.front();
    record.erase(record.begin());
  }

  void set_right_record(CompoundRecord &record) override
  {
    set_record(record);
  }

  void set_record(Record *record)
  {
    this->record_ = record;
  }

  void set_schema(const Table *table, const std::vector<FieldMeta> *fields)
  {
    table_ = table;
    this->speces_.reserve(fields->size());
    for (const FieldMeta &field : *fields) {
      speces_.push_back(new TupleCellSpec(new FieldExpr(table, &field)));
    }
  }

  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }

    const TupleCellSpec *spec = speces_[index];
    FieldExpr *field_expr = (FieldExpr *)spec->expression();
    const FieldMeta *field_meta = field_expr->field().meta();
    cell.set_type(field_meta->type());
    cell.set_data(this->record_->data() + field_meta->offset());
    cell.set_length(field_meta->len());
    return RC::SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    const char *table_name = field.table_name();
    if (0 != strcmp(table_name, table_->name())) {
      return RC::NOTFOUND;
    }

    const char *field_name = field.field_name();
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &field = field_expr->field();
      if (0 == strcmp(field_name, field.field_name())) {
	return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }

  Record &record()
  {
    return *record_;
  }

  const Record &record() const
  {
    return *record_;
  }

  void get_record(CompoundRecord &record) const override
  {
    record.emplace_back(record_);
  }

private:
  Record *record_ = nullptr;
  const Table *table_ = nullptr;
  std::vector<TupleCellSpec *> speces_;
};

/*
class CompositeTuple : public Tuple
{
public:
  int cell_num() const override; 
  RC  cell_at(int index, TupleCell &cell) const = 0;
private:
  int cell_num_ = 0;
  std::vector<Tuple *> tuples_;
};
*/

class ProjectTuple : public Tuple
{
public:
  ProjectTuple() = default;
  virtual ~ProjectTuple()
  {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_tuple(Tuple *tuple)
  {
    this->tuple_ = tuple;
  }

  void add_cell_spec(TupleCellSpec *spec)
  {
    speces_.push_back(spec);
  }
  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::GENERIC_ERROR;
    }
    if (tuple_ == nullptr) {
      return RC::GENERIC_ERROR;
    }

    const TupleCellSpec *spec = speces_[index];
    return spec->expression()->get_value(*tuple_, cell);
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    return tuple_->find_cell(field, cell);
  }
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::NOTFOUND;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }

  void get_record(CompoundRecord &record) const override
  {
    tuple_->get_record(record);
  }

  void set_record(CompoundRecord &record) override
  {
    tuple_->set_record(record);
  }

  void set_right_record(CompoundRecord &record) override
  {
    tuple_->set_right_record(record);
  }

private:
  std::vector<TupleCellSpec *> speces_;
  Tuple *tuple_ = nullptr;
};

//创建新类，合并后元组集。直接模仿RowTuple。作用是将两个元组集合并成一个元组集实现join，是select-tables的核心之一
//辅助JoinOperator类的实现
class JoinedTuple : public Tuple {
public:
  JoinedTuple() = default;
  virtual ~JoinedTuple() = default;
  
  // JoinedTuple(Tuple *left, Tuple *right)
  // {
  //   init(left, right);
  // }
  JoinedTuple(Tuple *left, Tuple *right) : left_tup_(left), right_tup_(right)
  {}

  //根据左右两个元组的类型，组合成行元组成员joinedtuple_
  // void init(Tuple *left, Tuple *right)
  // {
  //   //joinedtuple类型元组
  //   JoinedTuple *new_joinedtuple=nullptr;
  //   //rowtuple类型元组
  //   RowTuple *new_rowtuple=nullptr;
  //   //先处理原左元组，使用dynamic_case将left的Tuple类型指针转换为RowTuple类型
  //   new_rowtuple=dynamic_cast<RowTuple *>(left);
  //   if(new_rowtuple!=nullptr){
  //     //添加到joinedtuple_
  //     joinedtuple_.push_back(new_rowtuple);
  //   }else{
  //     //使用dynamic_case将left的Tuple类型指针转换为JoinedTuple类型
  //     new_joinedtuple=dynamic_cast<JoinedTuple *>(left);
  //     if(new_joinedtuple!=nullptr){
  //       joinedtuple_.insert(joinedtuple_.end(),new_joinedtuple->joinedtuple_.begin(),new_joinedtuple->joinedtuple_.end());
  //     }
  //     //将joined_tuple中的joinedtuple_的所有元素添加到主joinedtuple_中，从原集合的最后添加，添加整个
  //   }
  //   //再处理原右元组
  //   new_rowtuple=dynamic_cast<RowTuple *>(right);
  //   //添加到joinedtuple_
  //   joinedtuple_.push_back(new_rowtuple);
  // }
  void init(Tuple *left, Tuple *right)
  {
    left_tup_ = left;
    right_tup_ = right;
  }

  void set_right_record(CompoundRecord &record) override
  {
    right_tup_->set_right_record(record);
  }

  //根据给定的record向joinedtuple_中的各个RowTuple对象设置记录
  // void set_record(const std::vector<Record *> &records)
  // {
  //   //从头带尾迭代即可
  //   auto old_record=joinedtuple_.begin();
  //   auto new_record=records.begin();
  //   for (;old_record!=joinedtuple_.end();old_record++,new_record++++){
  //     //直接调用RowTuple的设置记录方法
  //     (*old_record)->set_record(*new_record);
  //   }
  // }

  void set_record(CompoundRecord &record) override
  {
    left_tup_->set_record(record);
    right_tup_->set_record(record);
  }

  //获取所有元组的单元格总数，要遍历每个元组并调用同名方法
  int cell_num() const override
  {
    // int num = 0;
    // for (auto tup : joinedtuple_) {
    //   num += tup->cell_num();
    // }
    // return num;
    return left_tup_->cell_num() + right_tup_->cell_num();
  }
  //返回指定索引位置的单元格值
  RC cell_at(int index, TupleCell &cell) const override
  {
    // Tuple *tuple = nullptr;
    // int real_index = -1;
    // RC rc = find_tuple_and_index(index, tuple, real_index);
    // if (RC::SUCCESS != rc) {
    //   return rc;
    // }
    // return tuple->cell_at(real_index, cell);
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }
    int num = left_tup_->cell_num();
    if (index < num) {
      return left_tup_->cell_at(index, cell);
    }
    return right_tup_->cell_at(index - num, cell);
  }
  //根据给定的字段查找JoinedTuple中的单元格
  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    // for (auto tup : joinedtuple_) {
    //   if (RC::SUCCESS == tup->find_cell(field, cell)) {
    //     return RC::SUCCESS;
    //   }
    // }
    // return RC::NOTFOUND;
    if (RC::SUCCESS != left_tup_->find_cell(field, cell)) {
      return right_tup_->find_cell(field, cell);
    }
    return RC::SUCCESS;
  }
  //返回JoinedTuple中指定索引位置的单元格规格
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    // Tuple *tuple = nullptr;
    // int real_index = -1;
    // RC rc = find_tuple_and_index(index, tuple, real_index);
    // if (RC::SUCCESS != rc) {
    //   return rc;
    // }
    // return tuple->cell_spec_at(real_index, spec);
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }
    int num = left_tup_->cell_num();
    if (index < num) {
      return left_tup_->cell_spec_at(index, spec);
    }
    return right_tup_->cell_spec_at(index - num, spec);
  }
  void get_record(CompoundRecord &record) const override
  {
    left_tup_->get_record(record);
    right_tup_->get_record(record);
  }

private:
  //组合后元组集，是一个动态数组，每个元素是join后元组
  std::vector<RowTuple *> joinedtuple_;
  Tuple *left_tup_;
  Tuple *right_tup_;
};