package main

import (
	"testing"
)

func Test_fibonaci(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name    string
		args    args
		wantRet int
	}{
		{
			name:    "fibonaci(9)",
			args:    args{i: 9},
			wantRet: 34,
		},
		{
			name:    "fibonaci(1)",
			args:    args{i: 1},
			wantRet: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRet := fibonaci(tt.args.i); gotRet != tt.wantRet {
				t.Errorf("fibonaci() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}

func Test_add(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "add(1, 2)",
			args: args{a: 1, b: 2},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := add(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_addThenSubtract(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "addThenSubtract(1, 2)",
			args: args{a: 1, b: 2},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := addThenSubtract(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("addThenSubtract() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_squareThenAdd(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "squareThenAdd(1, 2)",
			args: args{a: 1, b: 2},
			want: 9,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := addThenSquare(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("squareThenAdd() = %v, want %v", got, tt.want)
			}
		})
	}
}
