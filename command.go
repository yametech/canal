/*
Copyright 2019 yametech.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	length := len(c.D) - 1
	cap := len(c.D) - 1
	args := make([]interface{}, length, cap)
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
