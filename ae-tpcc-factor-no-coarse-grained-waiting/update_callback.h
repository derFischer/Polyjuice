#ifndef _UPDATE_CALLBACK_H_
#define _UPDATE_CALLBACK_H_
#include <string>

class update_callback {
public:
  virtual ~update_callback() {}

  update_callback(std::string &retrieved_value, std::string &new_value) : retrieved_value(retrieved_value),
                                                                          new_value(new_value) {}

  virtual void *invoke() = 0;

protected:
  std::string &retrieved_value;
  std::string &new_value;
};

#endif /* _UPDATE_CALLBACK_H_ */