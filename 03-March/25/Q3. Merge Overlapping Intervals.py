# Q3. Merge Overlapping Intervals
# Unsolved
# character backgroundcharacter
# Stuck somewhere?
# Use any given hint and get it resolved
# Use Hint.
# Problem Description
# Given a collection of intervals, merge all overlapping intervals.

# Problem Constraints
# 1 <= Total number of intervals <= 100000.

# Input Format
# First argument is a list of intervals.

# Output Format
# Return the sorted list of intervals after merging all the overlapping intervals.

# Example Input
# Input 1:
# [1,3],[2,6],[8,10],[15,18]


# Example Output
# Output 1:
# [1,6],[8,10],[15,18]


# Example Explanation
# Explanation 1:
# Merge intervals [1,3] and [2,6] -> [1,6].
# so, the required answer after merging is [1,6],[8,10],[15,18].
# No more overlapping intervals present.


# Definition for an interval.
# class Interval:
#     def __init__(self, s=0, e=0):
#         self.start = s
#         self.end = e

class Solution:
    # @param intervals, a list of Intervals
    # @return a list of Interval
    def merge(self, intervals):
        ret = []
        sm, em = intervals[0].start, intervals[0].end
        for i in range(1,len(intervals)):
            if intervals[i].end < em:
                em = max(em, intervals[i].end)
            else:
                ret.append(Interval(sm, em))
                sm = intervals[i].start
                em = intervals[i].end
        
        return ret