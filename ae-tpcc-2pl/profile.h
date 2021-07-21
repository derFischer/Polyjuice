#ifndef IC3PROFILE
#define IC3PROFILE

#include "macros.h"

struct ic3_profile {

  enum level
  {
    LEVEL0 = 0,
    LEVEL1,
    LEVEL2,
    LEVEL3,
    LEVEL4,
    LEVEL5,
    LEVEL6,
    LEVEL7,
    LEVEL8,
    LEVEL9,
    LEVEL10,
    LEVEL11,
    LEVEL12,
    LEVEL13,
    LEVEL14,
    LEVEL15,
    LEVEL16,
    LEVEL17,
    LEVEL18,
    LEVEL19,
    LEVEL20,
  };

#define MAX_PROFILE_LEVEL 21

  uint64_t data[MAX_PROFILE_LEVEL];
  uint32_t count; 
  uint32_t padding; 

  ic3_profile()
  {
    for(int i = 0; i < MAX_PROFILE_LEVEL; i++)
      data[i] = 0;
    count = 0;
  }

  
} CACHE_ALIGNED;

#endif