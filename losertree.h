#ifndef LOSER_TREE_H
#define LOSER_TREE_H

#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t file_index;      // which file (1 to maxParts)
    size_t item_index;      // current position in pointerMap
    char *current_string;   // current string being compared
} LoserNode;

typedef struct {
    LoserNode *tree;        // tree[0] = winner, tree[1..n] = internal nodes
    LoserNode *leaves;      // leaves[1..n] = actual file data
    size_t num_files;
    int (*compare)(const char *, const char *);
} LoserTree;

LoserTree* loser_tree_init(size_t num_files, int (*compare)(const char *, const char *)) {
    LoserTree *lt = (LoserTree *)malloc(sizeof(LoserTree));
    if (!lt) return NULL;
    
    // tree needs num_files internal nodes + 1 for winner at position 0
    lt->tree = (LoserNode *)calloc(num_files + 1, sizeof(LoserNode));
    lt->leaves = (LoserNode *)calloc(num_files + 1, sizeof(LoserNode));
    
    if (!lt->tree || !lt->leaves) {
        free(lt->tree);
        free(lt->leaves);
        free(lt);
        return NULL;
    }
    
    lt->num_files = num_files;
    lt->compare = compare;
    
    // Initialize leaves to represent all files
    for (size_t i = 1; i <= num_files; i++) {
        lt->leaves[i].file_index = i;
        lt->leaves[i].item_index = 0;
        lt->leaves[i].current_string = NULL;
    }
    
    return lt;
}

void loser_tree_free(LoserTree *lt) {
    if (lt) {
        free(lt->tree);
        free(lt->leaves);
        free(lt);
    }
}

// Build initial tree with all current strings from each file
void loser_tree_build(LoserTree *lt) {
    // Initialize tree nodes to losers
    for (size_t i = 1; i <= lt->num_files; i++) {
        lt->tree[i].file_index = i;
    }
    
    // Rebuild from leaves up
    for (int i = (int)lt->num_files - 1; i >= 1; i--) {
        LoserNode *left_winner = &lt->tree[2 * i];
        LoserNode *right_winner = &lt->tree[2 * i + 1];
        
        int cmp = lt->compare(left_winner->current_string, right_winner->current_string);
        
        if (cmp <= 0) {
            lt->tree[i].file_index = right_winner->file_index;
            left_winner->file_index = left_winner->file_index; // left is winner
        } else {
            lt->tree[i].file_index = left_winner->file_index;
            right_winner->file_index = right_winner->file_index; // right is winner
        }
    }
}

// Update tree after consuming a string from file_index
void loser_tree_update(LoserTree *lt, size_t file_index, char *new_string) {
    lt->leaves[file_index].current_string = new_string;
    
    int pos = (int)file_index;
    int parent = pos / 2;
    
    while (parent >= 1) {
        LoserNode *sibling = &lt->tree[pos ^ 1];
        
        int cmp = lt->compare(new_string, sibling->current_string);
        
        if (cmp <= 0) {
            // Current file wins, sibling is loser
            int tmp = lt->tree[parent].file_index;
            lt->tree[parent].file_index = sibling->file_index;
            sibling->file_index = tmp;
        } else {
            // Sibling wins, current file is loser
            int tmp = lt->tree[parent].file_index;
            lt->tree[parent].file_index = file_index;
            file_index = tmp;
        }
        
        pos = parent;
        parent = pos / 2;
    }
}

// Get current winner
static inline size_t loser_tree_winner(LoserTree *lt) {
    return lt->tree[0].file_index;
}

#endif // LOSER_TREE_H