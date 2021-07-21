#include <limits>

#include "ldb_wrapper.h"
#include "../macros.h"

using namespace std;

ldb_wrapper::ldb_wrapper()
{
	printf("Hello World!!!!!\n");
}

ldb_wrapper::~ldb_wrapper()
{

}

void *
ldb_wrapper::new_txn(
    uint64_t txn_flags,
    str_arena &arena,
    void *buf, TxnProfileHint hint)
{
  return NULL;
}

bool
ldb_wrapper::commit_txn(void *p)
{
  return true;
}

void
ldb_wrapper::abort_txn(void *p)
{
}

abstract_ordered_index *
ldb_wrapper::open_index(const string &name, size_t value_size_hint, bool mostly_append)
{
 
  return new ldb_ordered_index(name, value_size_hint, mostly_append);
}

void
ldb_wrapper::close_index(abstract_ordered_index *idx)
{
 
}

ldb_ordered_index::~ldb_ordered_index()
{
 
}

bool
ldb_ordered_index::get(
    void *txn,
    const string &key,
    string &value,
    size_t max_bytes_read)
{
  return true;
}

const char *
ldb_ordered_index::put(
    void *txn,
    const string &key,
    const string &value)
{
  return NULL;
}
