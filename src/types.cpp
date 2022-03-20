#include "types.h"

#include <cmath>
#include <cassert>

void SubmissionInput::serialize(std::ofstream& out) const {
    out.write((char *)&timestamp_s, sizeof(TTimestamp));
    out.write((char *)&ip,          sizeof(TIPAddress));
    out.write((char *)&slotId,      sizeof(TSlotId));
    out.write((char *)&userId,      sizeof(TUserId));

    // serialize string as length and data
    uint32_t length = (uint32_t) word.length();
    out.write((char *)&length, sizeof(uint32_t));
    out.write(word.c_str(), length);
}

void SubmissionInput::deserialize(std::ifstream& in) {
    in.read((char *)&timestamp_s, sizeof(TTimestamp));
    in.read((char *)&ip,          sizeof(TIPAddress));
    in.read((char *)&slotId,      sizeof(TSlotId));
    in.read((char *)&userId,      sizeof(TUserId));

    // deserialize word, reading the length first
    uint32_t length;
    in.read((char *)&length, sizeof(uint32_t));
    char buffer[kMaxWordLength + 1];
    in.read(buffer, length);
    word = std::string(buffer, length);
}

bool convertIPAddress(const std::string & ipAddress, TIPAddress & ip) {
    auto parts = std::vector<uint32_t>();
    parts.reserve(4);

    auto part = std::string();
    for (auto c : ipAddress) {
        if (c == '.') {
            if (part.empty()) {
                return false;
            }
            parts.push_back(std::stoi(part));
            part.clear();
        } else {
            part += c;
        }
    }
    if (part.empty()) {
        return false;
    }
    parts.push_back(std::stoi(part));

    if (parts.size() != 4) {
        return false;
    }

    ip = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
    return true;
}

void Slot::update() {
    statistics.topVoted.clear();

    for (auto& word : words) {
        statistics.topVoted.push_back(std::make_pair(word.first, word.second.votes_mv));
    }

    std::sort(statistics.topVoted.begin(), statistics.topVoted.end(),
              [](const std::pair<std::string, uint32_t>& a,
                 const std::pair<std::string, uint32_t>& b) {
                  return a.second > b.second;
              });
}

int64_t State::votesNeeded(int32_t slots) const {
    return std::ceil(std::pow(slots, 1.0/0.6));
}

int32_t State::activeSlots(int64_t votes) const {
    return std::max(kInitialSlots, (int32_t) std::pow(votes, 0.6));
}

int32_t State::activeSlots() const {
    return activeSlots(statistics.votes);
}

void State::init() {
    slots.resize(kInitialSlots);
}

void State::submit(SubmissionInput input, CBOnNewPeriodStart&& onNewPeriodStart) {
    if (input.slotId >= (TSlotId) slots.size()) {
        fprintf(stderr, "Invalid slot id: %d, current active slots: %lu\n", input.slotId, slots.size());
        return;
    }

    const int32_t newPeriodId = input.timestamp_s/secondsInPeriod;
    if (curPeriodId != newPeriodId) {
        if (onNewPeriodStart) {
            onNewPeriodStart(curPeriodId);
        }

        curPeriodId = newPeriodId;

        submissions.clear();
    }

    if (auto itIP = submissions.find(input.ip); itIP == submissions.end()) {
        // this IP submits for the frist time
        statistics.uniqueIPs++;
    }

    slots[input.slotId].statistics.lastSubmissionTimestamp_s = input.timestamp_s;

    {
        auto & curIP = submissions[input.ip];
        if (auto itSlot = curIP.find(input.slotId); itSlot == curIP.end()) {
            // this IP submits for the frist time for that slot
            statistics.votes++;
            slots[input.slotId].statistics.votes++;
        }

        {
            auto & curSlot = curIP[input.slotId];
            if (auto itUser = curSlot.find(input.userId); itUser == curSlot.end()) {
                // remove old contributions for this slot
                if (curSlot.size() > 0) {
                    const int64_t v_mv = std::round(1000.0/curSlot.size());
                    for (const auto & sub : curSlot) {
                        slots[input.slotId].words[sub.second.word].votes_mv -= v_mv;
                        assert(slots[input.slotId].words[sub.second.word].votes_mv >= 0);
                    }
                }

                // new submission
                curSlot.emplace(input.userId, Submission { std::move(input.word) });
                statistics.submissions++;
                slots[input.slotId].statistics.submissions++;

                // recompute contributions for this slot
                {
                    const int64_t v_mv = std::round(1000.0/curSlot.size());
                    for (const auto & sub : curSlot) {
                        slots[input.slotId].words[sub.second.word].votes_mv += v_mv;
                    }
                }
            } else {
                // remove old contribution by this user
                const int64_t v_mv = std::round(1000.0/curSlot.size());
                slots[input.slotId].words[itUser->second.word].votes_mv -= v_mv;
                assert(slots[input.slotId].words[itUser->second.word].votes_mv >= 0);

                // edit existing submission
                itUser->second.word = std::move(input.word);

                // recompute contribution by this user
                slots[input.slotId].words[itUser->second.word].votes_mv += v_mv;
            }
        }
    }

    // update active slots
    {
        const auto nSlotsNew = activeSlots();
        if (nSlotsNew > (int32_t) slots.size()) {
            slots.resize(nSlotsNew);
            //printf("Resized slots to %d\n", nSlotsNew);
        }
    }
}

void State::update() {
    for (auto & slot : slots) {
        slot.update();
    }
}

void State::output(const std::string & filename, size_t nTopWordsPerSlot) const {
    std::ofstream file(filename);

    file << "{" << std::endl;
    file << "  \"votes\": " << statistics.votes << "," << std::endl;
    file << "  \"submissions\": " << statistics.submissions << "," << std::endl;
    file << "  \"next\": " << votesNeeded(activeSlots() + 1) << "," << std::endl;
    file << "  \"ips\": " << statistics.uniqueIPs << "," << std::endl;

    file << "  \"slots\": [" << std::endl;
    for (uint32_t i = 0; i < slots.size(); ++i) {
        const auto & slot = slots[i];
        file << "    {" << std::endl;
        file << "      \"id\": " << i << "," << std::endl;
        file << "      \"votes\": " << slot.statistics.votes << "," << std::endl;
        file << "      \"submissions\": " << slot.statistics.submissions << "," << std::endl;

        file << "      \"top\": [" << std::endl;
        const size_t nTopWords = std::min(nTopWordsPerSlot, slot.statistics.topVoted.size());
        for (size_t j = 0; j < nTopWords; ++j) {
            const auto & word = slot.statistics.topVoted[j];
            file << "        {" << std::endl;
            file << "          \"word\": \"" << word.first << "\"," << std::endl;
            file << "          \"votes\": " << word.second << std::endl;
            file << "        }";
            if (j < nTopWords - 1) {
                file << ",";
            }
            file << std::endl;
        }
        file << "      ]" << std::endl;
        file << "    }" << std::endl;
        if (i < slots.size() - 1) {
            file << ",";
        }
    }
    file << "  ]" << std::endl;
    file << "}" << std::endl;
}

namespace Gen {

TTimestamp timestamp() {
    TTimestamp result;

    static TTimestamp lastTimestamp = 0;
    result = lastTimestamp++;

    return result;
}

TIPAddress ip() {
    return rand()%std::numeric_limits<TIPAddress>::max();
}

TSlotId slotId(int32_t n) {
    return rand()%n;
}

TUserId userId() {
    return rand()%std::numeric_limits<TUserId>::max();
}

TWord word() {
    static const std::vector<std::string> kWords = {
        "apple",
        "banana",
        "orange",
        "pear",
        "grape",
        "strawberry",
        "watermelon",
        "cherry",
        "peach",
        "kiwi",
        "pineapple",
        "mango",
        "coconut",
        "avocado",
        "papaya",
        "plum",
        "peach",
        "lemon",
        "lime",
    };

    return kWords[rand()%kWords.size()];
}

SubmissionInput submissionInput(int32_t n) {
    SubmissionInput result {
        timestamp(),
        ip(),
        slotId(n),
        userId(),
        word(),
    };

    return result;
}

}

namespace Print {

void submissionInput(const SubmissionInput& input) {
    printf("SubmissionInput(ip=%d.%d.%d.%d, slotId=%d, userId=%d, word=%s)\n",
        input.ip >> 24, (input.ip >> 16) & 0xff, (input.ip >> 8) & 0xff, input.ip & 0xff,
        input.slotId,
        input.userId%256,
        input.word.c_str());
}

}
