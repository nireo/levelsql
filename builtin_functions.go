package levelsql

import (
	"fmt"
	"strings"
)

type builtinFunc func(exec *exec, row *row, args []node) (value, error)

var builtinFuncs = map[string]builtinFunc{
	"lower":      builtinLower,
	"upper":      builtinUpper,
	"equal_fold": builtinEqualFold,
}

func builtinLower(exec *exec, row *row, args []node) (value, error) {
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

func builtinUpper(exec *exec, row *row, args []node) (value, error) {
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

func builtinEqualFold(exec *exec, row *row, args []node) (value, error) {
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
