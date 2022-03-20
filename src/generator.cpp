#include "generator.h"

#include <cmath>

namespace Gen {

// generate random normally distributed number
float frandNormal() {
    float u1 = (float)rand() / (float)RAND_MAX;
    float u2 = (float)rand() / (float)RAND_MAX;
    float randStdNormal = sqrt(-2.0*log(u1))*cos(2.0*M_PI*u2);
    return randStdNormal;
}

float frandGaussian(float mean) {
    return mean + 0.33f*mean*frandNormal();
}

float frandGaussian(float mean, float sigma) {
    return mean + sigma*frandNormal();
}

struct Submissions::Impl {
    struct User {
        TIPAddress ip;
        TUserId userId;
    };

    int32_t curPeriodId = 0;
    int32_t submissionId = 0;

    int32_t usersAtPeriod(TPeriodId periodId) const {
        return 10.0*std::pow(parameters.coeffUsersScale, periodId);
    }

    Parameters parameters;

    std::vector<User> users;
    std::vector<SubmissionInput> submissions;
};

Submissions::Submissions(Parameters parameters) : m_impl(new Impl()) {
    m_impl->parameters = parameters;
}

Submissions::~Submissions() = default;

SubmissionInput Submissions::next(int32_t nSlots) {
    if (m_impl->submissionId >= (int32_t) m_impl->submissions.size()) {
        m_impl->submissions.clear();
        m_impl->submissionId = 0;

        {
            const auto nOld = m_impl->users.size();
            m_impl->users.resize(m_impl->usersAtPeriod(m_impl->curPeriodId));

            TIPAddress ip;
            int32_t nRepeat = 0;

            for (int i = nOld; i < (int) m_impl->users.size(); ++i) {
                if (nRepeat <= 0) {
                    ip = Gen::ip();
                    nRepeat = std::max(1.0f, frandGaussian(m_impl->parameters.avgUsersPerIP));
                }

                m_impl->users[i].ip = ip;
                m_impl->users[i].userId = Gen::userId();
                nRepeat--;
            }
        }

        for (int i = 0; i < (int) m_impl->users.size(); ++i) {
            const auto & user = m_impl->users[i];

            const auto nSubmissions = std::max(1.0f, frandGaussian(m_impl->parameters.avgSubmissionsPerUserPerPeriod));
            for (int j = 0; j < (int) nSubmissions; ++j) {
                SubmissionInput submission;
                submission.ip = user.ip;
                submission.slotId = frandGaussian(nSlots, 3); // vote primarily for the last slots
                if (submission.slotId >= nSlots) {
                    submission.slotId = 2*nSlots - submission.slotId - 1;
                }
                if (submission.slotId < 0) {
                    submission.slotId = 0;
                }
                submission.userId = user.userId;
                submission.word = Gen::word();

                m_impl->submissions.push_back(std::move(submission));
            }
        }

        {
            const double avgSubmissionTime = double(State::secondsInPeriod)/m_impl->submissions.size();
            for (int i = 0; i < (int) m_impl->submissions.size(); ++i) {
                auto & submission = m_impl->submissions[i];

                submission.timestamp_s = State::secondsInPeriod*m_impl->curPeriodId + i*avgSubmissionTime;
            }
        }

        printf("Generated %d submissions for period %d. Users = %d, Slots = %d\n",
               (int) m_impl->submissions.size(),
               m_impl->curPeriodId,
               (int) m_impl->users.size(), nSlots);

        m_impl->curPeriodId++;
    }

    return m_impl->submissions[m_impl->submissionId++];
}

void Submissions::setPeriod(TPeriodId periodId) {
    m_impl->curPeriodId = periodId;
}

}
