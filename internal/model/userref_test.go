package model

import "testing"

func TestUserRef_IsZero(t *testing.T) {
	tests := []struct {
		name string
		ref  UserRef
		want bool
	}{
		{
			name: "zero value is zero",
			ref:  UserRef{},
			want: true,
		},
		{
			name: "only PlatformID set",
			ref:  UserRef{PlatformID: 42},
			want: false,
		},
		{
			name: "only Login set",
			ref:  UserRef{Login: "octocat"},
			want: false,
		},
		{
			name: "both PlatformID and Login set",
			ref:  UserRef{PlatformID: 42, Login: "octocat"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ref.IsZero(); got != tt.want {
				t.Errorf("UserRef.IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
