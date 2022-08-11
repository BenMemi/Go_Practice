package apple

import "testing"

func Test_fubtract(t *testing.T) {
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
			name: "fubtract(1, 2)",
			args: args{a: 1, b: 2},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Fubtract(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("fubtract() = %v, want %v", got, tt.want)
			}
		})
	}
}
