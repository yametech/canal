package canal

import (
	"errors"
	"strings"
)

// Command all the command combinations
type Command struct {
	D []string
}

func (c *Command) Set(v ...string) { c.D = v }

func (c *Command) String() string {
	return strings.Join(c.D, " ")
}

func (c *Command) Args() []interface{} {
	args := make([]interface{}, len(c.D)-1, len(c.D)-1)
	for i := range c.D[1:] {
		args[i] = c.D[i+1]
	}
	return args
}

func buildStrCommand(s string) []string {
	return strings.Split(s, "\r\n")
}

func NewCommand(args ...string) (*Command, error) {
	if len(args) == 0 {
		return nil, errors.New("Empty args.")
	}
	return &Command{D: args}, nil
}
