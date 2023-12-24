#include "sql/operator/join_operator.h"
#include "storage/common/table.h"
#include "rc.h"

JoinOperator::JoinOperator(Operator *left, Operator *right):left_(left),right_(right) {}
JoinOperator::~JoinOperator(){}

//模仿delete_operator.cpp
RC JoinOperator::open()
{
  RC rc = RC::SUCCESS;
  if (RC::SUCCESS != (rc = left_->open())) {
    rc = RC::INTERNAL;
    LOG_WARN("JoinOperater child left open failed!");
  }
  if (RC::SUCCESS != (rc = right_->open())) {
    rc = RC::INTERNAL;
    LOG_WARN("JoinOperater child right open failed!");
  }
  Tuple *left_tuple = left_->current_tuple();
  Tuple *right_tuple = right_->current_tuple();
  tuple_.init(left_tuple, right_tuple);
  // assert(RC::SUCCESS == left_->next());
  return rc;
  /*
  RC rc=left_->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  rc=right_->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  Tuple* tuple_left=left_->current_tuple();
  Tuple* tuple_right=right_->current_tuple();
  tuple_.init(tuple_left,tuple_right);
  return rc;
  */
}

RC JoinOperator::next()
{
  RC rc = RC::SUCCESS;
  if (is_first_) {
    rc = left_->next();
    is_first_ = false;
    if (RC::SUCCESS != rc) {
      return rc;
    }
  }
  rc = right_->next();

  if (RC::SUCCESS == rc) {
    return rc;
  }
  if (RC::RECORD_EOF != rc) {
    // LOG_ERROR
    return rc;
  }
  rc = left_->next();
  if (RC::SUCCESS == rc) {
    assert(RC::SUCCESS == right_->close());
    assert(RC::SUCCESS == right_->open());
    //assert(RC::SUCCESS == right_->next());
    return next();
  }
  // LOG_ERROR
  return rc;
  /*
  RC rc = right_->next();
  if(rc==RC::SUCCESS){
    return rc;
  }
  rc=left_->next();
  return next();
  */
}

RC JoinOperator::close()
{
  left_->close();
  right_->close();
  return RC::SUCCESS;
}

Tuple * JoinOperator::current_tuple()
{
  return &tuple_;
}