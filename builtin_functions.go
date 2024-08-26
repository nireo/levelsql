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
	"lower":      builtinLower,
	"upper":      builtinUpper,
	"equal_fold": builtinEqualFold,
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
		return value{}, fmt.Errorf("equalFold takes 2 arguments, got: %d", len(args))
	}

	vala, err := exec.executeExpression(args[0], row)
	if err != nil {
		return value{}, err
	}

	valb, err := exec.executeExpression(args[1], row)
	if err != nil {
		return value{}, err
	}

	return value{
		boolVal: strings.EqualFold(vala.asStr(), valb.asStr()),
		ty:      boolVal,
	}, nil
}
