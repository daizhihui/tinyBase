#include <cstdio>
#include <iostream>
#include <sys/times.h>
#include <sys/types.h>
#include <cassert>
#include <unistd.h>
#include "redbase.h"
#include "ql.h"

void QueryNode::addChild(QueryNode* n)
{
  n->remove_parent_ptr(); // if n has a parent, disconnect it
  n->parent() = this;
  _children.push_back(n);
}



void QueryNode::remove_parent_ptr()
{
  if (parent()) {
    auto i = parent()->arg_begin();
    while (*i != this) ++i; // Must be found, don't check arg_end()
    parent()->arg_erase(i);
  }
}

inline QueryNode::const_iterator QueryNode::begin() const
{
 return const_iterator(this,this);
}

inline QueryNode::const_iterator QueryNode::end() const
{
 return const_iterator(nullptr,this);
}

// Prefix increment
 nodeIterato& nodeIterator::operator++() {
  if (nptr->is_leaf()) {
   // This is a leaf node, so we need to climb up
   for (;;) {
    if (nptr == root) {
     nptr = nullptr;
     break;
    }
    // note: if nptr is not root, it must have a parent
    const QueryNode* par = nptr->parent();
    // Determine which child we are
    auto next = par->arg_begin();
    for ( ; *next != nptr ; ++next); // no bounds check: nptr is in array
    ++next; // bring to next
    if (next != par->arg_end()) {
     // Branch down to next child
     nptr = *next;
     break;
    } else {
     // We were the last child of parent node, so continue up
     nptr = par;
    }
   }
  } else {
   // Not a leaf node, so move down one step to the left
   nptr = nptr->arg_first();
  }
  return *this;
 }

 // Prefix decrement
 nodeIterato& nodeIterator::operator--() {
   if (nptr) {
    // note: -- on first element is undefined => we may safely move up if not left
    if (nptr == nptr->parent()->arg_first()) {
     // nptr is first child => move up
     nptr = nptr->parent();
    } else {
     // nptr is not first child => move up one step, then traverse down
     // find pointer from parent
     auto prev = --nptr->parent()->arg_end();
     for ( ; *prev != nptr; --prev);
     --prev; // previous from nptr (prev can't be argv.front())
     nptr = *prev;
     // Now traverse down right most
     while (!nptr->is_leaf()) nptr = nptr->arg_last();
    }
   } else {
    // nptr at end, so we need to use root to get to last element
    for (nptr = root; !nptr->is_leaf(); ) {
     nptr = nptr->arg_last();
    }
   }
   return *this;
  }
