package sour

import "testing"

func Test_square(t *testing.T) {
	type args struct {
		x int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "square(9)",
			args: args{x: 9},
			want: 81,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Square(tt.args.x); got != tt.want {
				t.Errorf("square() = %v, want %v", got, tt.want)
			}
		})
	}
}
