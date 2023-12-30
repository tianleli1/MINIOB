#include "sql/operator/join_operator.h"

JoinOperator::JoinOperator(Operator *left, Operator *right) : left_(left), right_(right)
{
  //将rht_it_设置为rht_容器的末尾迭代器。现在容器是空的，所以它的末尾就是未来装有元素的容器的begin
  rht_it_ = rht_.end();
}
JoinOperator::~JoinOperator()
{
  //遍历rht_每个元素compound_rcd的每个记录rcd，然后删除这些记录。
  for(auto &cpd_rcd : rht_){
    for(auto rcd : cpd_rcd){
      delete rcd;
    }
  }
}

RC JoinOperator::open()
{
  //打开连接操作(join)，依次打开左右算子然后调用JoinedTuple的方法进行连接
  RC rc = RC::SUCCESS;
  rc=left_->open();
  rc=right_->open();
  //这里只初始化JoinedTuple的左右元组（赋值），还没有join两个表
  tuple_.init(left_->current_tuple(),right_->current_tuple());
  return rc;
}

//从右表算子right_中获取记录存储于rht_中。它仅会在首次执行连接操作时被调用一次
RC JoinOperator::get_right_table()
{
  RC rc = RC::SUCCESS;
  //从右表算子中获取记录，直到遇到记录结束
  while (RC::SUCCESS == (rc = right_->next())) {
    //创建CompoundRecord对象存储从右表算子中获取的一组记录，实际是一行
    CompoundRecord cpd_rcd;
    //从右操作数的当前元组中获取记录，并存储在cpd_rcd中
    right_->current_tuple()->get_record(cpd_rcd);
    //对cpd_rcd中的每个记录进行深拷贝，并将拷贝后的记录添加到rht_容器
    for (auto &rcd_ptr : cpd_rcd) {
      rcd_ptr = new Record(*rcd_ptr);
    }
    rht_.emplace_back(cpd_rcd);
  }
  //将rht_it_设置为rht_容器的起始迭代器
  rht_it_ = rht_.begin();
  //如果读到记录结束说明正确获取
  if (RC::RECORD_EOF == rc) {
    return RC::SUCCESS;
  }
  return rc;
}

//获取join的下一个元组。此处实现了join操作的笛卡儿积
RC JoinOperator::next()
{
  RC rc = RC::SUCCESS;
  if (is_first_) {
    //首次调用先尝试取左边的元组，对应笛卡尔积从左边开始
    rc = left_->next();
    //以后不会首先取左元组，直到右元组取尽会主动尝试取左元组
    is_first_ = false;
    //如果取不到左元组，返回相应的错误码
    if (RC::SUCCESS != rc) {
      return rc;
    }
    //调用get_right_table函数从右算子取记录，此函数只会在此调用一次
    rc = get_right_table();
    //如果获取右表记录失败，直接返回相应的错误码
    if (RC::SUCCESS != rc) {
      return rc;
    }
  }

  //每次调用next，下面这段代码执行连接。使用迭代器来设置当前元组tuple_
  //如果迭代器没有到达rht_的末尾，表示还有未处理的元组，将右表的记录设置到当前元组中并返回SUCCESS
  if (rht_.end() != rht_it_) {
    CompoundRecord temp(*rht_it_);
    //将当前未处理的元组设置为当前元组tuple_的右侧记录
    tuple_.set_right_record(temp);
    //向前移动，更新迭代器以准备获取下一组记录
    rht_it_++;
    return RC::SUCCESS;
  }
  // //然后取右元组
  // rc = right_->next();
  // if (RC::SUCCESS == rc) {
  //   return rc;
  // }
  // if (RC::RECORD_EOF != rc) {
  //   //出错
  //   return rc;
  // }
  //rht_it_到达了右表rht_的末尾，右表全部添加完。尝试调用左算子的next。这表示笛卡尔积中，一个左元组对应的全部右元组都取完了
  rc = left_->next();
  if (RC::SUCCESS == rc) {
    //如果能成功取到左元组，表示继续笛卡尔积。重置右表迭代器，再取一轮右元组。递归
    // right_->close();
    // right_->open();
    rht_it_ = rht_.begin();
    return next();
  }
  //全部取完了。笛卡尔积结束
  return rc;
}

RC JoinOperator::close()
{
  //关闭连接操作
  left_->close();
  right_->close();
  return RC::SUCCESS;
}

Tuple * JoinOperator::current_tuple()
{
  return &tuple_;
}