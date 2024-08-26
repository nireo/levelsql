package levelsql

import (
	"fmt"
	"strings"
)

// we need this because without it there is a circular dependency using the exec
// struct directly.
type expressionExecutor interface {
	executeExpression(expr node, row *row) (value, error)
}

type builtinFunc func(exec expressionExecutor, row *row, args []node) (value, error)

var builtinFuncs = map[string]builtinFunc{
	"lower":         builtinLower,
	"upper":         builtinUpper,
	"equal_fold":    builtinEqualFold,
	"string_repeat": builtinStringRepeat,
	"concat":        builtinConcat,
}

func executeArgs(exec expressionExecutor, row *row, args []node) ([]value, error) {
	values := make([]value, 0, len(args))
	for _, expr := range args {
		executed, err := exec.executeExpression(expr, row)
		if err != nil {
			return nil, err
		}

		values = append(values, executed)
	}

	return values, nil
}

func builtinLower(exec expressionExecutor, row *row, args []node) (value, error) {
	if len(args) != 1 {
		return value{}, fmt.Errorf("lower takes 1 argument, got: %d", len(args))
	}

	valToLower, err := exec.executeExpression(args[0], row)
	if err != nil {
		return value{}, err
	}

	return value{
		stringVal: strings.ToLower(valToLower.asStr()),
		ty:        stringVal,
	}, nil
}

func builtinUpper(exec expressionExecutor, row *row, args []node) (value, error) {
	if len(args) != 1 {
		return value{}, fmt.Errorf("upper takes 1 argument, got: %d", len(args))
	}

	valToLower, err := exec.executeExpression(args[0], row)
	if err != nil {
		return value{}, err
	}

	return value{
		stringVal: strings.ToUpper(valToLower.asStr()),
		ty:        stringVal,
	}, nil
}

func builtinEqualFold(exec expressionExecutor, row *row, args []node) (value, error) {
	if len(args) != 2 {
		return value{}, fmt.Errorf("equal_fold takes 2 arguments, got: %d", len(args))
	}

	vals, err := executeArgs(exec, row, args)
	if err != nil {
		return value{}, err
	}

	return value{
		boolVal: strings.EqualFold(vals[0].asStr(), vals[1].asStr()),
		ty:      boolVal,
	}, nil
}

func builtinStringRepeat(exec expressionExecutor, row *row, args []node) (value, error) {
	if len(args) != 2 {
		return value{}, fmt.Errorf("string_repeat takes 2 arguments, got: %d", len(args))
	}

	vals, err := executeArgs(exec, row, args)
	if err != nil {
		return value{}, err
	}

	return value{
		stringVal: strings.Repeat(vals[0].asStr(), int(vals[1].asInt())),
		ty:        stringVal,
	}, nil
}

func builtinConcat(exec expressionExecutor, row *row, args []node) (value, error) {
	if len(args) != 2 {
		return value{}, fmt.Errorf("concat takes 2 arguments, got %d", len(args))
	}

	vals, err := executeArgs(exec, row, args)
	if err != nil {
		return value{}, err
	}

	return value{
		stringVal: vals[0].asStr() + vals[1].asStr(),
		ty:        stringVal,
	}, nil
}
