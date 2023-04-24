class Solution:
    # @param A : list of integers
    # @return an integer
    def solve(self, A):
        n = len(A)
        
        if n == 1:
            return A[0]
        
        low, high = 0, n-1
        while low <= high:
            mid = (low + high) // 2
            if  mid == n-1 and  A[mid] > A[mid-1]:
                return A[mid]
            if A[mid] < A[mid+1]:
                low = mid + 1
            else:
                high = mid - 1
                # Keep Updating till the low > high
                if A[mid] >= A[mid+1] and A[mid] >= A[mid-1]:
                    return A[mid]
                

# Chat GPT
def find_peak_element(A):
    n = len(A)
    left, right = 0, n - 1
    while left <= right:
        mid = (left + right) // 2
        if (mid == 0 or A[mid] >= A[mid - 1]) and (mid == n - 1 or A[mid] >= A[mid + 1]):
            return A[mid]
        elif mid > 0 and A[mid] < A[mid - 1]:
            right = mid - 1
        else:
            left = mid + 1

# GPT 4
def find_peak_element(arr):
    n = len(arr)

    # If the array has only one element, return it as the peak
    if n == 1:
        return arr[0]

    # Check if the first element is a peak
    if arr[0] >= arr[1]:
        return arr[0]

    # Check if the last element is a peak
    if arr[n - 1] >= arr[n - 2]:
        return arr[n - 1]

    # Iterate through the rest of the array and check for peaks
    for i in range(1, n - 1):
        if arr[i] >= arr[i - 1] and arr[i] >= arr[i + 1]:
            return arr[i]

# Test the function
A = [1, 3, 20, 4, 1, 0]
print(find_peak_element(A))  # Output: 20
