package main

import (
	"fmt"
	"strconv"
	"strings"
)

type intList []int

func (v *intList) String() string { return fmt.Sprint([]int(*v)) }
func (v *intList) Set(raw string) error {
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return fmt.Errorf("invalid message id %q", part)
		}
		*v = append(*v, n)
	}
	return nil
}

type selection struct {
	IDs      intList
	FromID   int
	Type     string
	Input    intList
	Topic    int
	Reply    int
	Filter   string
	Explicit bool
}

func (s *selection) validate() error {
	primary := 0
	if len(s.IDs) > 0 {
		primary++
	}
	if s.FromID > 0 {
		primary++
	}
	if s.Type != "" || len(s.Input) > 0 {
		primary++
	}
	if primary > 1 {
		return fmt.Errorf("--id, --from-id, and --type/--input are mutually exclusive")
	}
	if s.Topic > 0 && s.Reply > 0 {
		return fmt.Errorf("--topic and --reply are mutually exclusive")
	}
	if s.Type != "" {
		switch s.Type {
		case "id", "time":
			if len(s.Input) != 2 || s.Input[0] > s.Input[1] {
				return fmt.Errorf("--type %s requires --input MIN,MAX", s.Type)
			}
		case "last":
			if len(s.Input) != 1 {
				return fmt.Errorf("--type last requires --input COUNT")
			}
		default:
			return fmt.Errorf("--type must be id, time, or last")
		}
	} else if len(s.Input) > 0 {
		return fmt.Errorf("--input requires --type")
	}
	s.Explicit = primary > 0
	return nil
}
