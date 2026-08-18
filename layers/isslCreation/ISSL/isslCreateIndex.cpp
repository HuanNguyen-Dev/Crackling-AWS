/*
 * Scorer-compatible ISSL index creator with serverless worker modes.
 *
 * Legacy:
 *   isslCreateIndex sites.txt sequence-length slice-width output.issl
 * Catalog worker:
 *   isslCreateIndex catalog sites.txt sequence-length output.catalog
 * Slice worker:
 *   isslCreateIndex slice input.catalog slice-width slice-index output.slice
 *
 * A catalog is an ordered run-length encoding of the sorted input.  Slice
 * workers are independent: one Lambda may build each slice.  The fragment
 * format uses fixed-width fields so orchestration code does not depend on the
 * host size_t ABI.  Legacy .issl output deliberately retains the original
 * native Linux-64 layout expected by isslScoreOfftargets.
 */

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include <stdint.h>

using std::map;
using std::string;
using std::vector;

namespace {

const char CATALOG_MAGIC[8] = {'I','S','S','L','C','A','T','1'};
const char SLICE_MAGIC[8] = {'I','S','S','L','S','L','C','1'};

struct Record {
    uint64_t signature;
    uint32_t occurrences;
};

struct Catalog {
    uint64_t sequenceLength;
    uint64_t sequenceCount;
    vector<Record> records;
};

void fail(const string &message) {
    std::fprintf(stderr, "Error: %s\n", message.c_str());
    std::exit(EXIT_FAILURE);
}

uint64_t parseUnsigned(const char *text, const char *name) {
    if (!text || !*text || *text == '-') fail(string("invalid ") + name);
    errno = 0;
    char *end = NULL;
    unsigned long long value = std::strtoull(text, &end, 10);
    if (errno == ERANGE || !end || *end != '\0') fail(string("invalid ") + name);
    return static_cast<uint64_t>(value);
}

void checkedWrite(const void *data, size_t size, size_t count, FILE *fp,
                  const char *description) {
    if (count && std::fwrite(data, size, count, fp) != count)
        fail(string("failed writing ") + description);
}

void checkedRead(void *data, size_t size, size_t count, FILE *fp,
                 const char *description) {
    if (count && std::fread(data, size, count, fp) != count)
        fail(string("failed reading ") + description);
}

FILE *openFile(const char *path, const char *mode) {
    FILE *fp = std::fopen(path, mode);
    if (!fp) fail(string("cannot open '") + path + "': " + std::strerror(errno));
    return fp;
}

uint8_t nucleotide(char base) {
    switch (base) {
        case 'A': return 0;
        case 'C': return 1;
        case 'G': return 2;
        case 'T': return 3;
        default: fail(string("invalid nucleotide '") + base + "'");
    }
    return 0;
}

uint64_t encode(const string &sequence) {
    uint64_t signature = 0;
    for (size_t i = 0; i < sequence.size(); ++i)
        signature |= static_cast<uint64_t>(nucleotide(sequence[i])) << (i * 2);
    return signature;
}

Catalog readSites(const char *path, uint64_t sequenceLength) {
    if (sequenceLength == 0 || sequenceLength > 32)
        fail("sequence length must be between 1 and 32");

    FILE *fp = openFile(path, "rb");
    Catalog catalog;
    catalog.sequenceLength = sequenceLength;
    catalog.sequenceCount = 0;
    string current, previous;
    current.reserve(static_cast<size_t>(sequenceLength));
    int ch;
    uint64_t line = 1;
    while ((ch = std::fgetc(fp)) != EOF) {
        if (ch == '\r') fail("CRLF input is unsupported; input must use LF line endings");
        if (ch != '\n') {
            if (current.size() >= sequenceLength)
                fail("line " + std::to_string(line) + " exceeds sequence length");
            (void)nucleotide(static_cast<char>(ch));
            current.push_back(static_cast<char>(ch));
            continue;
        }
        if (current.size() != sequenceLength)
            fail("line " + std::to_string(line) + " has the wrong sequence length");
        if (!previous.empty() && current < previous)
            fail("input must be lexicographically sorted (line " + std::to_string(line) + ")");
        if (!catalog.records.empty() && current == previous) {
            if (catalog.records.back().occurrences == UINT32_MAX)
                fail("one sequence occurs more than UINT32_MAX times");
            ++catalog.records.back().occurrences;
        } else {
            if (catalog.records.size() >= UINT32_MAX)
                fail("more than UINT32_MAX distinct sequences are unsupported by the scorer");
            catalog.records.push_back(Record{encode(current), 1});
        }
        if (catalog.sequenceCount == std::numeric_limits<uint64_t>::max())
            fail("sequence count overflow");
        ++catalog.sequenceCount;
        previous.swap(current);
        current.clear();
        ++line;
    }
    if (std::ferror(fp)) fail("failed reading input sites");
    std::fclose(fp);
    if (!current.empty()) fail("final sequence is not LF terminated");
    if (catalog.records.empty()) fail("input contains no sequences");
    return catalog;
}

void writeCatalog(const Catalog &catalog, const char *path) {
    FILE *fp = openFile(path, "wb");
    checkedWrite(CATALOG_MAGIC, 1, sizeof(CATALOG_MAGIC), fp, "catalog magic");
    uint64_t fields[3] = {catalog.sequenceLength, catalog.sequenceCount,
                          static_cast<uint64_t>(catalog.records.size())};
    checkedWrite(fields, sizeof(uint64_t), 3, fp, "catalog header");
    for (size_t i = 0; i < catalog.records.size(); ++i) {
        checkedWrite(&catalog.records[i].signature, sizeof(uint64_t), 1, fp, "catalog signature");
        checkedWrite(&catalog.records[i].occurrences, sizeof(uint32_t), 1, fp, "catalog occurrences");
    }
    if (std::fclose(fp) != 0) fail("failed closing catalog");
}

Catalog readCatalog(const char *path) {
    FILE *fp = openFile(path, "rb");
    char magic[8];
    checkedRead(magic, 1, sizeof(magic), fp, "catalog magic");
    if (std::memcmp(magic, CATALOG_MAGIC, sizeof(magic)) != 0) fail("invalid catalog magic/version");
    uint64_t fields[3];
    checkedRead(fields, sizeof(uint64_t), 3, fp, "catalog header");
    if (fields[0] == 0 || fields[0] > 32 || fields[2] == 0 || fields[2] > UINT32_MAX)
        fail("invalid catalog header");
    if (fields[2] > static_cast<uint64_t>(std::numeric_limits<size_t>::max()))
        fail("catalog is too large for this host");
    Catalog catalog;
    catalog.sequenceLength = fields[0];
    catalog.sequenceCount = fields[1];
    catalog.records.resize(static_cast<size_t>(fields[2]));
    uint64_t occurrenceSum = 0;
    for (size_t i = 0; i < catalog.records.size(); ++i) {
        checkedRead(&catalog.records[i].signature, sizeof(uint64_t), 1, fp, "catalog signature");
        checkedRead(&catalog.records[i].occurrences, sizeof(uint32_t), 1, fp, "catalog occurrences");
        if (!catalog.records[i].occurrences || occurrenceSum > UINT64_MAX - catalog.records[i].occurrences)
            fail("invalid catalog occurrence count");
        occurrenceSum += catalog.records[i].occurrences;
    }
    if (occurrenceSum != catalog.sequenceCount) fail("catalog occurrence total does not match header");
    if (std::fgetc(fp) != EOF) fail("catalog contains trailing data");
    std::fclose(fp);
    return catalog;
}

vector<vector<uint64_t> > makeSlice(const Catalog &catalog, uint64_t width, uint64_t index) {
    if (width == 0 || width >= 31) fail("slice width must be between 1 and 30 bits");
    uint64_t bitCount = catalog.sequenceLength * 2;
    if (bitCount % width != 0) fail("slice width must divide the encoded sequence width exactly");
    uint64_t count = bitCount / width;
    if (index >= count) fail("slice index is out of range");
    uint64_t limit = uint64_t(1) << width;
    if (limit > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) fail("slice is too large");
    vector<vector<uint64_t> > buckets(static_cast<size_t>(limit));
    uint64_t shift = width * index;
    uint64_t mask = limit - 1;
    for (size_t id = 0; id < catalog.records.size(); ++id) {
        uint64_t bucket = (catalog.records[id].signature >> shift) & mask;
        uint64_t packed = (static_cast<uint64_t>(catalog.records[id].occurrences) << 32) |
                          static_cast<uint32_t>(id);
        buckets[static_cast<size_t>(bucket)].push_back(packed);
    }
    return buckets;
}

void writeSlice(const Catalog &catalog, uint64_t width, uint64_t index, const char *path) {
    vector<vector<uint64_t> > buckets = makeSlice(catalog, width, index);
    FILE *fp = openFile(path, "wb");
    checkedWrite(SLICE_MAGIC, 1, sizeof(SLICE_MAGIC), fp, "slice magic");
    uint64_t fields[5] = {catalog.sequenceLength, width, index,
                          static_cast<uint64_t>(buckets.size()),
                          static_cast<uint64_t>(catalog.records.size())};
    checkedWrite(fields, sizeof(uint64_t), 5, fp, "slice header");
    for (size_t i = 0; i < buckets.size(); ++i) {
        uint64_t count = static_cast<uint64_t>(buckets[i].size());
        checkedWrite(&count, sizeof(count), 1, fp, "slice bucket count");
    }
    for (size_t i = 0; i < buckets.size(); ++i)
        checkedWrite(buckets[i].data(), sizeof(uint64_t), buckets[i].size(), fp, "slice bucket entries");
    if (std::fclose(fp) != 0) fail("failed closing slice fragment");
}

vector<uint64_t> mismatchMasks(int length, int mismatches) {
    vector<uint64_t> result;
    if (mismatches < length) {
        if (mismatches > 0) {
            vector<uint64_t> with = mismatchMasks(length - 1, mismatches - 1);
            vector<uint64_t> without = mismatchMasks(length - 1, mismatches);
            uint64_t high = uint64_t(1) << ((length - 1) * 2);
            for (size_t i = 0; i < with.size(); ++i) result.push_back(high + with[i]);
            result.insert(result.end(), without.begin(), without.end());
        } else result.push_back(0);
    } else {
        uint64_t mask = 0;
        for (int i = 0; i < length; ++i) mask |= uint64_t(1) << (i * 2);
        result.push_back(mask);
    }
    return result;
}

double localScore(uint64_t differences, size_t sequenceLength) {
    int positions[32], count = 0;
    for (size_t i = 0; i < sequenceLength; ++i)
        if ((differences >> (i * 2)) & 3) positions[count++] = static_cast<int>(i);
    if (!count) return 0.0;
    const double penalties[20] = {0.0,0.0,0.014,0.0,0.0,0.395,0.317,0.0,0.389,0.079,
                                  0.445,0.508,0.613,0.851,0.732,0.828,0.615,0.804,0.685,0.583};
    double first = 1.0;
    for (int i = 0; i < count; ++i) {
        if (positions[i] >= 20) fail("legacy MIT scoring only supports sequence length up to 20");
        first *= 1.0 - penalties[positions[i]];
    }
    double distance = 19.0;
    if (count > 1) {
        distance = 0.0;
        for (int i = 0; i + 1 < count; ++i) distance += positions[i + 1] - positions[i];
        distance /= count - 1;
    }
    return first * (1.0 / (((19.0 - distance) / 19.0) * 4.0 + 1.0)) *
           (1.0 / (count * count)) * 100.0;
}

void writeLegacy(const Catalog &catalog, uint64_t width, const char *path) {
    if (catalog.sequenceLength != 20) fail("legacy scorer-compatible output requires sequence length 20");
    if (width == 0 || width >= 31 || (catalog.sequenceLength * 2) % width != 0)
        fail("invalid legacy slice width");
    size_t sliceCount = static_cast<size_t>((catalog.sequenceLength * 2) / width);
    map<uint64_t, double> scores;
    int maxDistance = static_cast<int>(sliceCount) - 1;
    for (int d = 1; d <= maxDistance; ++d) {
        vector<uint64_t> masks = mismatchMasks(20, d);
        for (size_t i = 0; i < masks.size(); ++i) scores.insert(std::make_pair(masks[i], localScore(masks[i], 20)));
    }
    vector<vector<vector<uint64_t> > > slices;
    slices.reserve(sliceCount);
    for (size_t i = 0; i < sliceCount; ++i) slices.push_back(makeSlice(catalog, width, i));

    FILE *fp = openFile(path, "wb");
    size_t header[6] = {catalog.records.size(), static_cast<size_t>(catalog.sequenceLength),
                        static_cast<size_t>(catalog.sequenceCount), static_cast<size_t>(width),
                        sliceCount, scores.size()};
    checkedWrite(header, sizeof(size_t), 6, fp, "ISSL header");
    for (map<uint64_t, double>::const_iterator it = scores.begin(); it != scores.end(); ++it) {
        checkedWrite(&it->first, sizeof(uint64_t), 1, fp, "MIT score mask");
        checkedWrite(&it->second, sizeof(double), 1, fp, "MIT score value");
    }
    for (size_t i = 0; i < catalog.records.size(); ++i)
        checkedWrite(&catalog.records[i].signature, sizeof(uint64_t), 1, fp, "off-target signature");
    for (size_t s = 0; s < slices.size(); ++s)
        for (size_t b = 0; b < slices[s].size(); ++b) {
            size_t count = slices[s][b].size();
            checkedWrite(&count, sizeof(size_t), 1, fp, "slice list size");
        }
    for (size_t s = 0; s < slices.size(); ++s)
        for (size_t b = 0; b < slices[s].size(); ++b)
            checkedWrite(slices[s][b].data(), sizeof(uint64_t), slices[s][b].size(), fp, "slice entries");
    if (std::fclose(fp) != 0) fail("failed closing ISSL output");
}

void usage(const char *program) {
    std::fprintf(stderr,
        "Usage:\n  %s sites.txt sequence-length slice-width output.issl\n"
        "  %s catalog sites.txt sequence-length output.catalog\n"
        "  %s slice input.catalog slice-width slice-index output.slice\n", program, program, program);
}

} // namespace

int main(int argc, char **argv) {
    if (argc == 5 && std::strcmp(argv[1], "catalog") == 0) {
        Catalog catalog = readSites(argv[2], parseUnsigned(argv[3], "sequence length"));
        writeCatalog(catalog, argv[4]);
        return EXIT_SUCCESS;
    }
    if (argc == 6 && std::strcmp(argv[1], "slice") == 0) {
        Catalog catalog = readCatalog(argv[2]);
        writeSlice(catalog, parseUnsigned(argv[3], "slice width"),
                   parseUnsigned(argv[4], "slice index"), argv[5]);
        return EXIT_SUCCESS;
    }
    if (argc == 5) {
        Catalog catalog = readSites(argv[1], parseUnsigned(argv[2], "sequence length"));
        writeLegacy(catalog, parseUnsigned(argv[3], "slice width"), argv[4]);
        return EXIT_SUCCESS;
    }
    usage(argv[0]);
    return EXIT_FAILURE;
}
