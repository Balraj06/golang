package main

import (
	"fmt"
	"math"
)

//get subset of array
func copy1(a []int, start int, stop int) []int {
	dest := make([]int, stop-start)
	var flag int = 0
	for i := start; i < stop; i++ {
		dest[flag] = a[i]
		flag++
	}
	//fmt.Println(flag)
	return dest[:]

}

func main() {
	a := []int{0, 0, 0}

	q := 2
	values := [][]int{}
	for i := 1; i <= q; i++ {
		fmt.Printf("Enter the elements for %v query ", i)
		var temp [3]int
		for j := 0; j < 3; j++ {
			fmt.Scanf("%d", &temp[j])
		}
		values = append(values, temp[:])

	}
	results := make([]int, q)
	index := 0

	for i := 0; i < 2; i++ {

		if values[i][0] == 1 {

			A := copy1(a, values[i][1], values[i][2])
			size := len(A)
			for j := 0; j < size; j++ {
				if j == 0 {
					A[j] = 1 + A[j]

				} else {
					A[j] = 1 + int(math.Max(float64(A[j]), float64(A[j-1])))

				}

			}
			results[index] = A[size-1]
			index++

		} else {

			A := copy1(a, values[i][1], values[i][2])
			size := len(A)
			for j := 0; j < size; j++ {
				if j == 0 {
					A[j] = 1 + A[j]

				} else {
					A[j] = 1 + int(math.Min(float64(A[j]), float64(A[j-1])))

				}

			}
			results[index] = A[size-1]
			index++

		}

	}
	fmt.Print(results)
}
