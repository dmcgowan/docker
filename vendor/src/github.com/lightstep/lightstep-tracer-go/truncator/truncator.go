package truncator

import (
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	ellipsis          = "…"
	overflowNodeBytes = len(ellipsis)
)

type Truncator struct {
	FieldMaxBytes int
	TotalMaxBytes int
}

func NewTruncator(fieldMaxBytes, totalMaxBytes int) *Truncator {
	return &Truncator{fieldMaxBytes, totalMaxBytes}
}

// See the comment for ValueToTruncatedJSONString; it does a BFT while
// building a tree structure of outputNodes, then calls
// outputNode.toJSONInterface() to do a final DFT on that intermediate
// structure to serialize.
type outputNode struct {
	// For any output node that's been populated by the BFT, exactly one of
	// these fields will be non-nil/true. Output nodes that have not been
	// populated turn into an ellipsis.
	stringVal *string
	sliceVal  []*outputNode
	mapVal    map[string]*outputNode
	isNil     bool
}

func newOutputNode() *outputNode {
	// Deliberately leave everything nil.
	return &outputNode{}
}

func (o *outputNode) toJSONInterface() interface{} {
	switch {

	// The non-default cases have been populated by the BFT.
	case o.stringVal != nil:
		return *o.stringVal
	case o.sliceVal != nil:
		rval := make([]interface{}, len(o.sliceVal))
		for i, elt := range o.sliceVal {
			rval[i] = elt.toJSONInterface()
		}
		return rval
	case o.mapVal != nil:
		rval := make(map[string]interface{}, len(o.mapVal))
		for k, elt := range o.mapVal {
			rval[k] = elt.toJSONInterface()
		}
		return rval
	case o.isNil:
		return nil

	default:
		// We never dequeued this node during the BFT; it's overflow, so
		// leave its string repr opaque.
		return ellipsis
	}
}

// Converts the native, arbitrary, in-memory interface{} to a JSON
// structure. This operates differently than Go's json.Marshal as it
// converts numeric values and all map key values to strings.
//
// Also, don't let the string-serialized length of any field get larger than
// FieldMaxBytes, and don't let the *total* length of the resulting string
// get much longer than TotalMaxBytes (emphasis on "much"). It's not a hard
// cap, as the implementation adds commas and colons and so forth which may
// not all be accounted for.
func (t *Truncator) TruncateToJSON(value interface{}) (string, error) {
	jsonInt := t.Truncate(value)
	jsonBytes, err := json.Marshal(jsonInt)
	if err == nil {
		return string(jsonBytes), nil
	}
	return "<error>", err
}

// The interesting (non-marshalling) part of ValueToTruncatedJSONString.
//
// NOTE: this *is* currently a thread-safe method.
//
func (t *Truncator) Truncate(value interface{}) interface{} {
	// IMPLEMENTATION NOTES: We do a breadth-first traversal (BFT) built around
	// the `queue` below, creating a simplified version of `value` as we go
	// that's anchored at `rootElt.out`.
	//
	// Every time we add something to the queue, we are either extending a
	// slice or adding a new element to a key-value map; when we do so, we
	// create a new queueElement and pass the sub-object of value via
	// queueElement.in and put the corresponding sub-output-object in
	// queueElement.out.
	//
	// When we add variable-length data to an outputNode, we increment the
	// `bytesSoFar` variable, taking care never to add more than FieldMaxBytes
	// at a time (and truncating field values as needed in the process). If
	// `bytesSoFar` ever exceeds TotalMaxBytes, the BFT stops, potentially
	// leaving many elements still in the queue.
	//
	// Once the BFT is finished, there is a comparatively simple/trivial
	// traversal of the outputNode tree anchored at `rootElt.out`; the only
	// subtle aspect is that any elements that were never *dequeued* from
	// `queue` will serialize as an ellipsis, even if they had substructure in
	// their corresponding subobject of `value`. In this way, we can omit
	// potentially large/huge swaths of the value [sub]object without
	// sacrificing the top-level fields.
	//
	// </longwinded_confusing_explanation>
	type queueElement struct {
		in  reflect.Value
		out *outputNode
	}
	rootElt := newOutputNode()

	// We do a BFS so we can more easily limit the size of the output (without
	// sacrificing breadth).
	queue := []*queueElement{&queueElement{reflect.ValueOf(value), rootElt}}

	bytesSoFar := 0
	canAcceptBytes := func(numBytes int) bool {
		return bytesSoFar+numBytes+(len(queue)*overflowNodeBytes) < t.TotalMaxBytes
	}

	safeIfaceToString := func(i interface{}) string {
		raw := fmt.Sprint(i)
		maxLen := t.TotalMaxBytes - bytesSoFar
		if t.FieldMaxBytes < maxLen {
			maxLen = t.FieldMaxBytes
		}
		if len(raw) > maxLen {
			// Truncate with an ellipsis.
			return raw[:maxLen] + ellipsis
		}
		return raw
	}

	// ("alwaysEnqueue", as opposed to the less trivial "maybeEnqueueKeyVal"
	// just below)
	alwaysEnqueue := func(in reflect.Value, out *outputNode) {
		queue = append(queue, &queueElement{in, out})
	}

	// Attempts to add `key: val` to `outMap` if there's still enough space
	// left per TotalMaxBytes. `key` is truncated per FieldMaxBytes as needed.
	//
	// Returns false if the caller should exit the BFT ASAP.
	maybeEnqueueKeyVal := func(key reflect.Value, val reflect.Value, outMap map[string]*outputNode) bool {
		if !key.CanInterface() {
			// Really should never reach this line, but we never want to crash
			// in this code.
			return false
		}
		keyStr := safeIfaceToString(key.Interface())
		if !canAcceptBytes(len(keyStr)) {
			outMap[ellipsis] = newOutputNode()
			return false
		}
		mapVal := newOutputNode()
		outMap[keyStr] = mapVal
		bytesSoFar += len(keyStr)
		if val.CanInterface() {
			queue = append(queue, &queueElement{reflect.ValueOf(val.Interface()), mapVal})
		} else {
			// NOTE: I can't imagine a situation where we'd take this branch,
			// but, since we can recover gracefully, we do.
			queue = append(queue, &queueElement{reflect.ValueOf("<opaque>"), mapVal})
		}
		return true
	}

	setSpecialStringVal := func(elt *queueElement, val string) {
		finalVal := "<" + val + ">"
		elt.out.stringVal = &finalVal
		// Don't forget to account for the space!
		bytesSoFar += len(finalVal)
	}

	for len(queue) > 0 && bytesSoFar < t.TotalMaxBytes {
		var curElt *queueElement
		curElt, queue = queue[0], queue[1:]

		v := curElt.in

		switch v.Kind() {

		case reflect.Ptr:
			if v.IsNil() {
				curElt.out.isNil = true
			} else {
				indir := reflect.Indirect(v)
				alwaysEnqueue(indir, curElt.out)
			}

		case reflect.Slice:
			fallthrough
		case reflect.Array:
			if v.IsNil() {
				curElt.out.isNil = true
			} else {
				curElt.out.sliceVal = []*outputNode{}
				// Output at most 10 elements (plus an ellipsis if necessary).
				i := 0
				for ; i < 5 && i < v.Len(); i++ {
					subOut := newOutputNode()
					curElt.out.sliceVal = append(curElt.out.sliceVal, subOut)
					alwaysEnqueue(v.Index(i), subOut)
				}
				if i < v.Len()-5 {
					i = v.Len() - 5
					subOut := newOutputNode()
					curElt.out.sliceVal = append(curElt.out.sliceVal, subOut)
					alwaysEnqueue(reflect.ValueOf(ellipsis), subOut)
				}
				for ; i < v.Len(); i++ {
					subOut := newOutputNode()
					curElt.out.sliceVal = append(curElt.out.sliceVal, subOut)
					alwaysEnqueue(v.Index(i), subOut)
				}
			}

		case reflect.Map:
			if v.IsNil() {
				curElt.out.isNil = true
			} else {
				curElt.out.mapVal = map[string]*outputNode{}
				keys := v.MapKeys()
				for _, key := range keys {
					if !maybeEnqueueKeyVal(key, v.MapIndex(key), curElt.out.mapVal) {
						break
					}
				}
			}

		case reflect.Struct:
			// TODO: this likely does not handle composite types / anonymous fields
			// correctly...I recall there's an extra hoop to jump through for those,
			// I believe (perhaps incorrectly).
			curElt.out.mapVal = map[string]*outputNode{}
			for i := 0; i < v.Type().NumField(); i++ {
				// Make sure the field is exported (otherwise reflection fails).
				if fType := v.Type().Field(i); len(fType.PkgPath) == 0 {
					if !maybeEnqueueKeyVal(reflect.ValueOf(fType.Name), v.Field(i), curElt.out.mapVal) {
						break
					}
				}
			}

		case reflect.Interface:
			// NB from go docs: "The argument must be a chan, func,
			// interface, map, pointer, or slice value... Note that IsNil is
			// not always equivalent to a regular comparison with nil in
			// Go."
			if v.IsNil() {
				curElt.out.isNil = true
			} else {
				alwaysEnqueue(v.Elem(), curElt.out)
			}
		case reflect.Chan:
			setSpecialStringVal(curElt, "chan")
		case reflect.Func:
			setSpecialStringVal(curElt, "func")
		case reflect.UnsafePointer:
			setSpecialStringVal(curElt, "pointer")
		case reflect.Invalid:
			// This is the case where a nil value appears in the
			// input... roughly because a bare nil has no type information,
			// and therefore can't really be put in any of the other cases.
			curElt.out.isNil = true

		// Simple leaf nodes:
		default:
			var rawStr string
			if v.CanInterface() {
				// It may be simpler to handle all indirections before the outer
				// switch statement to avoid specific checks like this.
				if v.Kind() == reflect.Ptr && v.IsNil() {
					rawStr = "nil"
					curElt.out.isNil = true
				} else {
					// (This truncates if need be)
					rawStr = safeIfaceToString(v.Interface())
				}
			} else {
				// NOTE: I can't imagine a situation where we'd take this
				// branch, but, since we can recover gracefully, we do.
				rawStr = "<unknown>"
			}
			curElt.out.stringVal = &rawStr
		}
	}

	// Convert the sanitized interface{} to JSON
	return rootElt.toJSONInterface()
}
