package common

import (
	"sort"
	"strings"
)

// StringPop is a struct containing a string and it's
// corresponding count (population).
type StringPop struct {
	String string
	Count  int
}

type StringPops []StringPop

func (l StringPops) Len() int           { return len(l) }
func (l StringPops) Less(i, j int) bool { return l[i].Count < l[j].Count }
func (l StringPops) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// EmbeddedLines returns a sorted slice of lines that
// are common in every input string when the string is split
// by \n. That is, the input string has embedded newlines.
func EmbeddedLines(inputs []string) []string {
	commonalities := make(map[string]int)
	for _, input := range inputs {
		split := strings.Split(input, "\n")
		for _, line := range split {
			commonalities[line] += 1
		}
	}

	max := 0
	for _, count := range commonalities {
		if count > max {
			max = count
		}
	}

	commonLines := []string{} // != nil

	for line, count := range commonalities {
		if count == max {
			commonLines = append(commonLines, line)
		}
	}

	sort.Strings(commonLines)

	return commonLines
}

// Strings returns an descending order slice of StringPop with
// each StringPop containing a line and the corresponding
// amount of times it it occurs in the input slice.
func Strings(input []string) []StringPop {
	commonalities := make(map[string]int)
	for _, line := range input {
		commonalities[line] += 1
	}

	lineCounts := []StringPop{}
	for line, count := range commonalities {
		lineCounts = append(lineCounts, StringPop{line, count})
	}

	sort.Sort(sort.Reverse(StringPops(lineCounts)))
	return lineCounts
}
