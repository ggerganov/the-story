#pragma once

#include "types.h"

#include <memory>

namespace Gen {

class Submissions {
public:
    struct Parameters {
        float coeffUsersScale = 1.10f;

        float avgUsersPerIP = 4.0f;
        float avgSubmissionsPerUserPerPeriod = 5.0f;
    };

    Submissions(Parameters parameters);
    ~Submissions();

    SubmissionInput next(int32_t nSlots);

    void setPeriod(TPeriodId periodId);

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

}
