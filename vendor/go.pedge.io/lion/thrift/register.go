package thriftlion

import (
	"fmt"
	"reflect"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	tStructReflectType = reflect.TypeOf((*thrift.TStruct)(nil)).Elem()
	nameToConstructor  = make(map[string]func() thrift.TStruct)
	lock               sync.RWMutex
)

func register(constructorObj interface{}) error {
	lock.Lock()
	defer lock.Unlock()

	constructor, err := castConstuctorObj(constructorObj)
	if err != nil {
		return err
	}
	name := getName(constructor())
	if _, ok := nameToConstructor[name]; ok {
		return fmt.Errorf("thriftlion: duplicate name %s", name)
	}
	nameToConstructor[name] = constructor
	return nil
}

func newTStruct(name string) (thrift.TStruct, error) {
	constructor, err := getConstructor(name)
	if err != nil {
		return nil, err
	}
	return constructor(), nil
}

func castConstuctorObj(constructorObj interface{}) (func() thrift.TStruct, error) {
	reflectValue := reflect.ValueOf(constructorObj)
	reflectType := reflectValue.Type()
	if reflectType.Kind() != reflect.Func {
		return nil, fmt.Errorf("thriftlion: %T is not a function", constructorObj)
	}
	if reflectType.NumIn() != 0 {
		return nil, fmt.Errorf("thriftlion: %T is not a constructor, must have zero arguments", constructorObj)
	}
	if reflectType.NumOut() != 1 {
		return nil, fmt.Errorf("thriftlion: %T is not a constructor, must have one return value", constructorObj)
	}
	if !reflectType.Out(0).AssignableTo(tStructReflectType) {
		return nil, fmt.Errorf("thriftlion: %T is not a constructor, must have a thrift.TStruct return value", constructorObj)
	}
	constructorOutReflectType := reflectType.Out(0).Elem()
	return func() thrift.TStruct {
		return reflect.New(constructorOutReflectType).Interface().(thrift.TStruct)
	}, nil
}

func getConstructor(name string) (func() thrift.TStruct, error) {
	lock.RLock()
	defer lock.RUnlock()

	constructor, ok := nameToConstructor[name]
	if !ok {
		return nil, fmt.Errorf("thriftlion: unknown name: %s", name)
	}
	return constructor, nil
}

func getName(tStruct thrift.TStruct) string {
	//reflectType := reflect.TypeOf(tStruct)
	//return fmt.Sprintf("%s.%s", reflectType.PkgPath(), reflectType.Name())
	return reflect.TypeOf(tStruct).String()
}
