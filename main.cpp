

#include "proj_service.h"

int main() {

    Driver* driver_ = new Driver();
    driver_->ParseCommandLine();
    delete driver_;
    return 0;

}