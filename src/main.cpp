// CMake-generated header containing timestamp of the build
#include "build_timestamp.h"

#include "types.h"
#include "utils.h"
#include "generator.h"

#include <cstdio>
#include <chrono>
#include <thread>
#include <regex>
#include <functional>
#include <filesystem>

// get files in folder by regex
std::vector<std::string> getFiles(const std::string & folder, const std::string & regex) {
    std::vector<std::string> files;

    for (const auto & entry : std::filesystem::directory_iterator(folder)) {
        if (entry.is_regular_file()) {
            std::regex r(regex);
            if (std::regex_match(entry.path().string(), r)) {
                files.push_back(entry.path().string());
            }
        }
    }

    return files;
}

// remove files
int removeFiles(const std::vector<std::string> & files) {
    int count = 0;

    for (const auto & file : files) {
        if (std::filesystem::remove(file)) {
            count++;
        }
    }

    return count;
}

// rename file
bool renameFile(const std::string & oldName, const std::string & newName) {
    try {
        std::filesystem::rename(oldName, newName);
    } catch (...) {
        fprintf(stderr, "Failed to rename file '%s' to '%s'\n", oldName.c_str(), newName.c_str());
        return false;
    }

    return true;
}

// serialize vector of SubmissionInput to a binary file
void serialize(const std::vector<SubmissionInput> & entries, const std::string & fileName) {
    std::ofstream file(fileName, std::ios::binary);

    // output number of elements
    const size_t numElements = entries.size();
    file.write((char *)&numElements, sizeof(numElements));

    // output each element
    for (const auto & entry : entries) {
        entry.serialize(file);
    }
}

// get SubmissionInput vector from a binary file
std::vector<SubmissionInput> deserializeAll(const std::string & fileName) {
    std::vector<SubmissionInput> entries;

    std::ifstream file(fileName, std::ios::binary);

    // read number of elements
    size_t numElements;
    file.read((char *)&numElements, sizeof(numElements));
    entries.resize(numElements);

    // read each element
    for (uint32_t i = 0; i < numElements; ++i) {
        entries[i].deserialize(file);
    }

    return entries;
}

SubmissionInput deserializeOne(const std::string & fileName) {
    std::ifstream file(fileName);

    SubmissionInput entry;
    file >> entry.timestamp_s;
    {
        std::string tmp;
        file >> tmp;
        convertIPAddress(tmp, entry.ip);
    }
    file >> entry.slotId;
    file >> entry.userId;
    file >> entry.word;

    return entry;
}

// command line arguments:
//    -h, --help : print help
//    -p, --prefix : input file prefix (e.g. "<prefix>-<periodId>.bin")
//   -os, --statistics-output : statistics output file (e.g. "stats.json")
//   -tv, --top-voted : number of top voted words to output for a slot (e.g. "10")
//   -ns, --num-submissions : number of submissions to generete (e.g. "100000")
//   -sf, --stats-file : output filename for statistics (e.g. "stats.json")
//  -sim, --simulation : run simulation
//   -df, --data-folder : data folder with binary input files
//   -pf, --pending-folder : folder with pending submissions

// define an enum for the command line arguments
// parse the command line arguments into a map of the enum and the argument as a string

enum CLIArgument {
    EHelp,
    EPrefix,
    EStatisticsOutput,
    ETopVoted,
    ENumSubmissions,
    EStatsFile,
    ESimulate,
    EDataFolder,
    EPendingFolder,
};

using TCLIArguments = std::map<CLIArgument, std::string>;

// return last processed periodId
TPeriodId processOld(State & state, const std::string & dataFolder, const std::string & prefix) {
    TPeriodId lastPeriodId = 0;

    // check if data folder exists
    if (!std::filesystem::exists(dataFolder)) {
        printf("Error: data folder \"%s\" does not exist\n", dataFolder.c_str());
        return lastPeriodId;
    }

    printf("Reading input files from '%s'\n", dataFolder.c_str());
    printf("Input file prefix: '%s'\n", prefix.c_str());

    // get all files in the data folder and sort them by name
    std::vector<std::string> files = getFiles(dataFolder, ".*" + prefix + "-\\d+\\.bin");

    // sort the files by name
    std::sort(files.begin(), files.end());

    printf("Found %lu files\n", files.size());
    for (const auto & file : files) {
        printf("Processing data from '%s' ...\n", file.c_str());
        std::vector<SubmissionInput> entries = deserializeAll(file);

        for (auto & entry : entries) {
            printf(" - processing word: '%s'\n", entry.word.c_str());
            state.submit(std::move(entry), [&](TPeriodId periodId) {
                lastPeriodId = periodId;
            });
        }
    }

    return lastPeriodId;
}

void writeStats(const State & state, const TCLIArguments & args) {
    const size_t nTopWordsPerSlot = args.count(CLIArgument::ETopVoted) ? std::stoi(args.at(CLIArgument::ETopVoted)) : 10;
    const std::string statsFile = args.count(CLIArgument::EStatsFile) ? args.at(CLIArgument::EStatsFile) : "stats.json";

    printf("Writing statistics to '%s'\n", statsFile.c_str());
    state.output(statsFile + ".tmp", nTopWordsPerSlot);
    renameFile(statsFile + ".tmp", statsFile);
}

int runSimulation(State state, TCLIArguments args) {
    TPeriodId lastPeriodId = 0;

    // if data folder and prefix are specified, read and process the input files
    if (args.count(CLIArgument::EDataFolder) && args.count(CLIArgument::EPrefix)) {
        lastPeriodId = processOld(state, args.at(CLIArgument::EDataFolder), args.at(CLIArgument::EPrefix));
    } else {
        printf("Skipping data processing.\n");
    }

    const auto tStart = std::chrono::high_resolution_clock::now();

    Gen::Submissions gen({});
    gen.setPeriod(lastPeriodId + 2);

    std::vector<SubmissionInput> curPeriodInput;
    const int nSubmissions = args.count(CLIArgument::ENumSubmissions) ? std::stoi(args.at(CLIArgument::ENumSubmissions)) : 1e6;

    for (int i = 0; i < nSubmissions; ++i) {
        const auto nSlots = state.slots.size();

        auto input = gen.next(nSlots);

        state.submit(SubmissionInput { input }, [&](TPeriodId periodId) {
            printf("New period has started, old period id: %d\n", periodId);

            if (curPeriodInput.empty()) return;

            if (args.count(CLIArgument::EDataFolder) && args.count(CLIArgument::EPrefix)) {
                const std::string dataFolder = args.at(CLIArgument::EDataFolder);
                const std::string prefix = args.at(CLIArgument::EPrefix);

                // serialize the current period input
                // periodId in the filename is padded to 5 digits
                const auto periodIdStr = std::to_string(periodId);
                const auto periodIdStrPadded = std::string(5 - periodIdStr.size(), '0') + periodIdStr;
                const std::string fileName = dataFolder + "/" + prefix + "-" + periodIdStrPadded + ".bin";

                serialize(curPeriodInput, fileName);
            }
        });

        curPeriodInput.push_back(std::move(input));
    }

    printf("Memory usage:      %f GB\n", Utils::getMemoryUsage()/1024.0/1024.0/1024.0);
    printf("Total votes:       %ld\n", state.statistics.votes);
    printf("Total submissions: %ld\n", state.statistics.submissions);

    {
        const auto tEnd = std::chrono::high_resolution_clock::now();
        printf("Time to process %d submissions: %.3f s\n", nSubmissions, std::chrono::duration<double>(tEnd - tStart).count());
    }

    state.update();
    writeStats(state, args);

    return 0;
}

int run(State state, TCLIArguments args) {
    TPeriodId lastPeriodId = 0;

    // if data folder and prefix are specified, read and process the input files
    if (args.count(CLIArgument::EDataFolder) && args.count(CLIArgument::EPrefix)) {
        lastPeriodId = processOld(state, args.at(CLIArgument::EDataFolder), args.at(CLIArgument::EPrefix));
    } else {
        printf("Skipping data processing.\n");
    }

    printf("Last period id: %d\n", lastPeriodId);

    std::vector<SubmissionInput> curPeriodInput;

    state.update();
    writeStats(state, args);

    while (true) {
        auto files = getFiles(args.at(CLIArgument::EPendingFolder), ".*s.*");

        if (files.size() > 0) {
            // sort the files by name
            std::sort(files.begin(), files.end());

            for (int i = 0; i < (int) files.size(); ++i) {
                const auto & fileName = files[i];
                printf("Processing pending submission from '%s' ...\n", fileName.c_str());

                auto entry = deserializeOne(fileName);

                printf("word = '%s'\n", entry.word.c_str());
                state.submit(entry, [&](TPeriodId periodId) {
                    printf("New period has started, old period id: %d\n", periodId);

                    if (curPeriodInput.empty()) {
                        printf("No submissions in current period.\n");
                        return;
                    }

                    if (args.count(CLIArgument::EDataFolder) && args.count(CLIArgument::EPrefix)) {
                        const std::string dataFolder = args.at(CLIArgument::EDataFolder);
                        const std::string prefix = args.at(CLIArgument::EPrefix);

                        // serialize the current period input
                        // periodId in the filename is padded to 5 digits
                        const auto periodIdStr = std::to_string(periodId);
                        const auto periodIdStrPadded = std::string(5 - periodIdStr.size(), '0') + periodIdStr;
                        const std::string fileName = dataFolder + "/" + prefix + "-" + periodIdStrPadded + ".bin";

                        printf("Writing %lu entries to file '%s'\n", curPeriodInput.size(), fileName.c_str());
                        serialize(curPeriodInput, fileName);
                        curPeriodInput.clear();
                    } else {
                        printf("Skipping input storage\n");
                    }
                });
                curPeriodInput.push_back(std::move(entry));
            }

            {
                printf("Removing %d files\n", (int) files.size());

                const auto nRemoved = removeFiles(files);
                if (nRemoved != (int) files.size()) {
                    fprintf(stderr, "Warning: %lu files were not removed\n", files.size() - nRemoved);
                }
            }

            state.update();
            writeStats(state, args);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char ** argv) {
    printf("Build time: %s\n", BUILD_TIMESTAMP);

    std::map<CLIArgument, std::string> args;

    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-h" || std::string(argv[i]) == "--help") {
            args[CLIArgument::EHelp] = "true";
        } else if (std::string(argv[i]) == "-p" || std::string(argv[i]) == "--prefix") {
            args[CLIArgument::EPrefix] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-os" || std::string(argv[i]) == "--statistics-output") {
            args[CLIArgument::EStatisticsOutput] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-tv" || std::string(argv[i]) == "--top-voted") {
            args[CLIArgument::ETopVoted] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-ns" || std::string(argv[i]) == "--num-submissions") {
            args[CLIArgument::ENumSubmissions] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-sf" || std::string(argv[i]) == "--stats-file") {
            args[CLIArgument::EStatsFile] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-sim" || std::string(argv[i]) == "--simulate") {
            args[CLIArgument::ESimulate] = "true";
        } else if (std::string(argv[i]) == "-df" || std::string(argv[i]) == "--data-folder") {
            args[CLIArgument::EDataFolder] = argv[i + 1];
            ++i;
        } else if (std::string(argv[i]) == "-pf" || std::string(argv[i]) == "--pending-folder") {
            args[CLIArgument::EPendingFolder] = argv[i + 1];
            ++i;
        }
    }

    if (args.empty() || args.count(CLIArgument::EHelp) > 0) {
        printf("Usage: %s -d <data-folder> -p <prefix> -os <stats-output-file> -tv <top-voted> -ns <num-submissions> -sf <stats-file>\n", argv[0]);
        printf("\n");
        printf("Options:\n");
        printf("    -h, --help : print help\n");
        printf("    -p, --prefix : input file prefix (e.g. \"<prefix>-<periodId>.bin\")\n");
        printf("   -os, --statistics-output : statistics output file (e.g. \"stats.json\")\n");
        printf("   -tv, --top-voted : number of top voted words to output for a slot (e.g. \"10\")\n");
        printf("   -ns, --num-submissions : number of submissions to generete (e.g. \"1e6\")\n");
        printf("   -sf, --stats-file : output filename for statistics (e.g. \"stats.json\")\n");
        printf("  -sim, --simulation : run simulation\n");
        printf("   -df, --data-folder : data folder with binary input files\n");
        printf("   -pf, --pending-folder : folder with pending submissions\n");
        printf("\n");
        printf("Example:\n");
        printf("  %s -df ./data -pf ./pending -p the-story -os stats.json -tv 10 -ns 100000 -sf stats.json\n", argv[0]);
        printf("\n");

        return 1;
    }

    State state;
    state.init();

    if (args.count(ESimulate) > 0) {
        runSimulation(std::move(state), std::move(args));
    } else {
        if (args.count(CLIArgument::EPendingFolder) == 0) {
            printf("Pending folder is not specified.\n");
            return 2;
        }
        run(std::move(state), std::move(args));
    }

    //for (int k = 0; k < std::log2(1e7); ++k) {
    //    printf(" - votes: %d, slots = %d\n", 1 << k, state.activeSlots(1 << k));
    //}

    return 0;
}
