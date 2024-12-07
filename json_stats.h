/* json_stats.h */
#ifndef JSON_STATS_H
#define JSON_STATS_H

#include <stdio.h>

// Structure for input statistics
typedef struct {
    size_t total_lines;
    size_t memory_required_bytes;
} InputStats;

// Structure for deduplication info
typedef struct {
    long unique_lines;
    long duplicate_lines;
    float deduplication_seconds;
} DeduplicationInfo;

// Structure for timings
typedef struct {
    float reading_seconds;
    float sorting_seconds;
    DeduplicationInfo deduplicating;
    float indexing_seconds;
    float searching_seconds;
    float writing_seconds;
    float total_time_seconds;
} Timings;

// Structure for search results
typedef struct {
    long unique_matches;
    long lines_written;
} SearchResults;

// Main structure to hold all data
typedef struct {
    char * inputFile;
    InputStats input_stats;
    Timings timings;
    char * outFile;
    SearchResults search_results;
} ProcessingStats;

typedef struct {
    char *error_message;      // Error description
    char *error_file;         // File where error occurred
    int error_line;          // Line number where error occurred
    char *error_function;     // Function where error occurred
    int error_code;          // Error code number
} ErrorStats;

typedef struct {
    char *input_file;         // Input file path
    size_t line_count;          // Number of lines
    size_t longest_line;        // Length of longest line
} LineStats;



// Function declaration
void output_json_stats(const ProcessingStats *stats, FILE *output);
void output_json_error(const ErrorStats *error, FILE *output);
void output_json_line_stats(const LineStats *stats, FILE *output);
#endif /* JSON_STATS_H */

/* json_stats.c */

void output_json_line_stats(const LineStats *stats, FILE *output) {
    fprintf(output, "{");
    fprintf(output, "\"input_file\":\"%s\",", stats->input_file);
    fprintf(output, "\"line_count\":%zu,", stats->line_count);
    fprintf(output, "\"longest_line\":%zu", stats->longest_line);
    fprintf(output, "}");
}

/* json_stats.c additions */
void output_json_error(const ErrorStats *error, FILE *output) {
    fprintf(output, "{");
    fprintf(output, "\"error\":{");
    fprintf(output, "\"message\":\"%s\",", error->error_message);
    fprintf(output, "\"file\":\"%s\",", error->error_file);
    fprintf(output, "\"line\":%d,", error->error_line);
    fprintf(output, "\"function\":\"%s\",", error->error_function);
    fprintf(output, "\"code\":%d", error->error_code);
    fprintf(output, "}}");
}

#include <stdio.h>

void output_json_stats(const ProcessingStats *stats, FILE *output) {
    fprintf(output, "{");
    fprintf(output, "\"input_file\":\"%s\",", stats->inputFile);
    fprintf(output, "\"input_stats\":{");
    fprintf(output, "\"total_lines\":%zu,", stats->input_stats.total_lines);
    fprintf(output, "\"memory_required_bytes\":%zu", stats->input_stats.memory_required_bytes);
    fprintf(output, "},");
    fprintf(output, "\"timings\":{");
    fprintf(output, "\"reading_seconds\":%.3f,", stats->timings.reading_seconds);
    fprintf(output, "\"sorting_seconds\":%.3f,", stats->timings.sorting_seconds);
    fprintf(output, "\"deduplicating\":{");
    fprintf(output, "\"unique_lines\":%ld,", stats->timings.deduplicating.unique_lines);
    fprintf(output, "\"duplicate_lines\":%ld,", stats->timings.deduplicating.duplicate_lines);
    fprintf(output, "\"deduplication_seconds\":%.3f", stats->timings.deduplicating.deduplication_seconds);
    fprintf(output, "},");
    fprintf(output, "\"indexing_seconds\":%.3f,", stats->timings.indexing_seconds);
    fprintf(output, "\"searching_seconds\":%.3f,", stats->timings.searching_seconds);
    fprintf(output, "\"writing_seconds\":%.3f,", stats->timings.writing_seconds);
    fprintf(output, "\"total_time_seconds\":%.3f", stats->timings.total_time_seconds);
    fprintf(output, "},");
    fprintf(output, "\"output_file\":\"%s\",", stats->outFile);
    fprintf(output, "\"search_results\":{");
    fprintf(output, "\"unique_matches\":%ld,", stats->search_results.unique_matches);
    fprintf(output, "\"lines_written\":%ld", stats->search_results.lines_written);
    fprintf(output, "}}");
}

