package main

import "fmt"

func main() {

	task := []temp{
		{2},
		{2},
		{4},
		{val: 5},
	}
	//function1(&task)
	//function2(task)
	//function3(&task)
	//function4(&task[0])
	function5(&task)
	fmt.Println(task)

}
func function5(t *[]temp) {
	size := len(*t)
	for i := 0; i < size; i++ {
		var b *temp
		b = &(*t)[i]
		b.val = 5555
	}
}

func function4(t *temp) {
	t.val = 1111
}
func function1(task *[]temp) {
	slice := *task
	for _, a := range slice {
		a.val = 9
	}
}
func function2(task []temp) {
	for _, a := range task {
		b := &a
		b.val = 777
	}
}
func function3(task *[]temp) {
	var slice = *task
	for _, a := range slice {
		b := &a
		b.val = 1000
	}
}

type temp struct {
	val int
}
