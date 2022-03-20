#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <fstream>
#include <functional>

using TPeriodId  = int32_t;
using TTimestamp = uint32_t;
using TIPAddress = uint32_t;
using TSlotId    = int32_t;
using TUserId    = uint16_t;
using TWord      = std::string;

constexpr auto kMaxWordLength = 32;
constexpr auto kInitialSlots = 3;

// this is the input data that we get for each new submission
struct SubmissionInput {
    TTimestamp timestamp_s;
    TIPAddress ip;
    TSlotId    slotId;
    TUserId    userId;
    TWord      word;

    // serialize to binary file
    void serialize(std::ofstream & out) const;

    // deserialize from binary file
    void deserialize(std::ifstream & in);
};

bool convertIPAddress(const std::string & ipAddress, TIPAddress & ip);

struct Slot {
    // per slot statistics
    struct Statistics {
        TTimestamp lastSubmissionTimestamp_s;

        int64_t votes;
        int64_t submissions;

        // top voted words
        std::vector<std::pair<TWord, int64_t>> topVoted;
    } statistics;

    struct WordData {
        int64_t votes_mv; // millivotes
    };

    // submitted words for the current slot
    std::unordered_map<TWord, WordData> words;

    void update();
};

struct State {
    using CBOnNewPeriodStart = std::function<void(TPeriodId periodId)>;

    // global statistics
    struct Statistics {
        int64_t votes       = 0;
        int64_t submissions = 0;
        int64_t uniqueIPs   = 0;

        // todo:
        // - time of last submission
        // - number of submissions during last N minutes
        // - histogram of submissions during last N minutes
    } statistics;

    // submission history is cleard when a new period starts
    static const int32_t secondsInPeriod = 1*24*3600;

    TPeriodId curPeriodId = 0;

    // the currently active word slots
    std::vector<Slot> slots;

    // the data that we store for each submission
    struct Submission {
        TWord word;
    };

    // all submissions
    std::unordered_map<TIPAddress,
        std::unordered_map<TSlotId,
            std::unordered_map<TUserId, Submission>>> submissions;

    int64_t votesNeeded(int32_t slots) const;
    int32_t activeSlots(int64_t votes) const;
    int32_t activeSlots() const;

    void init();

    void submit(SubmissionInput input, CBOnNewPeriodStart && onNewPeriodStart);

    // update slot statistics
    void update();

    void output(const std::string & filename, size_t nTopWordsPerSlot) const;
};

// generators of random input data, used for debugging/testing purposes
namespace Gen {

TTimestamp timestamp();
TIPAddress ip();
TSlotId    slotId(int32_t n);
TUserId    userId();
TWord      word();

SubmissionInput submissionInput(int32_t n);

}

// helper methods for printing stuff
namespace Print {

void submissionInput(const SubmissionInput & input);

}
