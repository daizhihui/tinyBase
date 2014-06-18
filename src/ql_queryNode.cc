#include <cstdio>
#include <iostream>
#include <sys/times.h>
#include <sys/types.h>
#include <cassert>
#include <unistd.h>
#include "redbase.h"
#include "ql.h"


inline QueryTree::const_iterator QueryTree::begin() const
{
    QueryNode *curr = root;
    // if the tree is not empty, the first node
      // inorder is the farthest node left from root
      if (curr != NULL)
        while (curr->left != NULL)
          curr = curr->left;
 return const_iterator(curr,this);
}

inline QueryTree::const_iterator QueryTree::end() const
{
 return const_iterator(nullptr,this);
}

// Prefix increment
 nodeIterator & nodeIterator::operator++() {
     QueryNode *p;

      if (nptr == NULL)
        {
        // ++ from end(). get the root of the tree
        nptr = tree->root;

        // error! ++ requested for an empty tree
        if (nptr == NULL)
          throw
            underflowError("nodeIterator iterator operator++ (): tree empty");

        // move to the smallest value in the tree,
        // which is the first node inorder
        while (nptr->left != NULL) {
          nptr = nptr->left;
          }
        }
      else
        if (nptr->right != NULL)
          {
           // successor is the furthest left node of
           // right subtree
           nptr = nptr->right;

           while (nptr->left != NULL) {
             nptr = nptr->left;
             }
          }
        else
          {
            p = nptr->parent;
            while (p != NULL && nptr == p->right)
              {
                nptr = p;
                p = p->parent;
              }
            nptr = p;
          }

      return *this;
 }

 // Prefix decrement
 nodeIterator& nodeIterator::operator--() {
   if (nptr) {
    // note: -- on first element is undefined => we may safely move up if not left
    if (nptr->left) {
     // nptr has child => move down
     nptr = nptr->left;
    } else {
     nptr = nptr->parent;
    }
   } else {
    // nptr at end, so we need to use root to get to last element
    for (nptr = root; nptr->left != NULL; ) {
     nptr = nptr->left;
    }
   }
   return *this;
  }
