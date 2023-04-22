# Problem Description
# You are given three positive integers, A, B, and C.
# Any positive integer is magical if divisible by either B or C.
# Return the Ath smallest magical number. Since the answer may be very large, return modulo 109 + 7.

# Problem Constraints
# 1 <= A <= 109
# 2 <= B, C <= 40000

# Input Format
# The first argument given is an integer A.
# The second argument given is an integer B.
# The third argument given is an integer C.

# Output Format
# Return the Ath smallest magical number. Since the answer may be very large, return modulo 109 + 7.

# Example Input
# Input 1:

#  A = 1
#  B = 2
#  C = 3

# Input 2:
#  A = 4
#  B = 2
#  C = 3


# Example Output
# Output 1:
#  2

# Output 2:
#  6

# Example Explanation
# Explanation 1:
#  1st magical number is 2.

# Explanation 2:
#  First four magical numbers are 2, 3, 4, 6 so the 4th magical number is 6.

class Solution:
    # @param A : integer
    # @param B : integer
    # @param C : integer
    # @return an integer
    def solve(self, A, B, C):

        def number_(num, B, C):
            return num//B + num//C - num//LCM(B,C)
            
        def GCD(x, y):
            while(y):
                x, y = y, x % y
            return x
    
        def LCM(x, y):
            return (x*y)//GCD(x,y)

        m = 1000000007
        res, left, right = -1, min(B,C), max(A*B, A*C)
        while left <= right:
            mid = (left+right)//2
            mid_magic_num = number_(mid, B, C)
            if mid_magic_num < A:
                left = mid + 1
            else:
                right = mid - 1
                if mid_magic_num == A:
                    res = mid
        return res%m