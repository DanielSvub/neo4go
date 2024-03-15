package neo4go

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/DanielSvub/anytype"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

/*
Interface for a connection to a database.

Extends:
  - io.Closer.
*/
type Connection interface {
	io.Closer
	Query(query string, params anytype.Object) (anytype.List, error)
	NewCollection(entity string) (Collection, error)
}

/*
Connection to the Neo4j database.

Implements:
  - Connector.
*/
type connection struct {
	driver  neo4j.DriverWithContext
	ctx     context.Context
	session neo4j.SessionWithContext
}

/*
Creates a new Neo4j connection.

Parameters:
  - address - address of the database (port 7687 assumed if not specified),
  - user - username,
  - password - password of the user.

Returns:
  - pointer to the created connection,
  - error if any occurred.
*/
func NewConnection(address string, user string, password string) (Connection, error) {
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://%s", address), neo4j.BasicAuth(user, password, ""))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	return &connection{driver, ctx, session}, nil
}

/*
Closes the connection.
*/
func (ego *connection) Close() error {

	if err := ego.session.Close(ego.ctx); err != nil {
		return err
	}

	return ego.driver.Close(ego.ctx)

}

/*
Performs a query over the Neo4j database.

Parameters:
  - query - text of the query in Cypher,
  - params - object containing variables used in the query.

Returns:
  - list of the query results,
  - error if any occurred.
*/
func (ego *connection) Query(query string, params anytype.Object) (anytype.List, error) {

	var paramDict map[string]any
	if params != nil {
		paramDict = params.Dict()
	}

	query = strings.TrimSpace(query)

	result, err := ego.session.Run(ego.ctx, query, paramDict)
	output := anytype.NewList()
	if err != nil {
		return output, err
	}

	for result.Next(ego.ctx) {
		record := result.Record()
		item := anytype.NewObject()
		for _, key := range record.Keys {
			value, ok := record.Get(key)
			if ok {
				switch val := value.(type) {
				case dbtype.Node:
					item.Set(key, anytype.NewObject(
						"identity", val.GetId(),
						"elementId", val.GetElementId(),
						"labels", val.Labels,
						"properties", val.GetProperties(),
					))
				default:
					item.Set(key, val)
				}
			}
		}
		output.Add(item)
	}

	if err := result.Err(); err != nil {
		return output, err
	}

	return output, nil
}

/*
One element of the Neo4j collection.

Promoted fields:
  - anytype.Object
*/
type node struct {
	*anytype.MapObject
	col      *collection
	id       string
	added    anytype.List
	modified anytype.List
	deleted  anytype.List
}

/*
Creates a new Neo4j node.

Parameters:
  - label - label of the node,
  - id - ElementId of the node,
  - obj - content of the node.

Returns:
  - pointer to the created node.
*/
func (ego *collection) newNode(id string, obj anytype.Object) *node {
	return &node{
		MapObject: obj.(*anytype.MapObject),
		col:       ego,
		id:        id,
		added:     anytype.NewList(),
		modified:  anytype.NewList(),
		deleted:   anytype.NewList(),
	}
}

/*
Creates a template of the node for the query.

Returns:
  - created template.
*/
func (ego *node) template() (result string) {
	result += "{"
	i := 0
	ego.ForEach(func(key string, _ any) {
		result += key + `:$` + key
		if i++; i < ego.Count() {
			result += ","
		}
	})
	result += "}"
	return
}

/*
Sets a values of the fields.
If the key already exists, the value is overwritten, if not, new field is created.
If one key is given multiple times, the value is set to the last one.

Parameters:
  - values... - any amount of key-value pairs to set.

Returns:
  - updated object (promoted field).
*/
func (ego *node) Set(values ...any) anytype.Object {
	if len(values)%2 == 0 {
		for i := 0; i < len(values); i += 2 {
			key, ok := values[i].(string)
			if !ok {
				panic("Keys have to be strings.")
			}
			if !ego.added.Contains(key) {
				if ego.KeyExists(key) {
					ego.modified.Add(key)
				} else {
					ego.added.Add(key)
				}
			}
		}
	}
	if !ego.col.modified.Contains(ego) {
		ego.col.modified.Add(ego)
	}
	return ego.MapObject.Set(values...)
}

/*
Deletes the fields with given keys.

Parameters:
  - keys... - any amount of keys to delete.

Returns:
  - updated object (promoted field).
*/
func (ego *node) Unset(keys ...string) anytype.Object {
	for _, key := range keys {
		if ego.added.Contains(key) {
			ego.added.Delete(ego.added.IndexOf(key))
		} else if ego.modified.Contains(key) {
			ego.modified.Delete(ego.added.IndexOf(key))
		} else {
			ego.deleted.Add(key)
		}
	}
	if !ego.col.modified.Contains(ego) {
		ego.col.modified.Add(ego)
	}
	return ego.MapObject.Unset(keys...)
}

/*
Refuses to delete all fields in the node and panics.
Overrides the method of the promoted field.
*/
func (ego *node) Clear() anytype.Object {
	panic("Cannot clear a Neo4j node.")
}

/*
Commits all changes to the database.

Returns:
  - updated object (promoted field).
*/
func (ego *node) Commit() *node {

	// Commiting set keys
	if !ego.added.Empty() || !ego.modified.Empty() {
		todo := ego.added.Concat(ego.modified)
		cypher := `MATCH (n:` + ego.col.label + `) WHERE elementId(n) = "` + ego.id + `" SET` + todo.
			ReduceStrings("", func(res, key string) string {
				var comma string
				if res != "" {
					comma = ","
				}
				return res + comma + ` n.` + key + ` = $` + key
			})
		_, err := ego.col.conn.Query(cypher, ego.Pluck(todo.StringSlice()...))
		if err != nil {
			panic(err)
		}
		ego.added.Clear()
	}

	// Commiting unset keys
	if !ego.deleted.Empty() {
		ego.deleted.
			ForEachString(func(key string) {
				_, err := ego.col.conn.Query(`
					MATCH (n:`+ego.col.label+`)
					WHERE elementId(n) = "`+ego.id+`"
					SET n.`+key+` = null
				`, nil)
				if err != nil {
					panic(err)
				}
			}).
			Clear()
	}

	return ego
}

/*
Interface for a collection.

Extends:
  - anytype.List.
*/
type Collection interface {
	anytype.List
	Commit() anytype.List
}

/*
Neo4j Collection.
Allows to alter the database by modifying the list and committing the changes.

Promoted fields:
  - anytype.List.
*/
type collection struct {
	*anytype.SliceList
	conn     *connection
	label    string
	added    anytype.List // List of nodes
	modified anytype.List // List of nodes
	deleted  anytype.List // List of IDs
}

/*
Creates a new Neo4j collection.

Parameters:
  - entity - label of the entity to get.

Returns:
  - updated list (promoted field).
*/
func (ego *connection) NewCollection(entity string) (Collection, error) {

	result, err := ego.Query("MATCH (n:"+entity+") RETURN n", nil)
	if err != nil {
		return nil, err
	}

	col := &collection{
		conn:     ego,
		label:    entity,
		added:    anytype.NewList(),
		modified: anytype.NewList(),
		deleted:  anytype.NewList(),
	}

	col.SliceList = result.MapObjects(func(x anytype.Object) any {
		return col.newNode(x.GetObject("n").GetString("elementId"),
			x.GetObject("n").GetObject("properties").(*anytype.MapObject))
	}).(*anytype.SliceList)

	return col, nil

}

/*
Adds new elements at the end of the collection.
Overrides the method of the promoted field.

Parameters:
  - values... - any amount of elements to add.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Add(values ...any) anytype.List {
	for _, value := range values {
		_, ok := value.(anytype.Object)
		if !ok {
			panic("Only object can be added to the collection.")
		}
		node := ego.newNode("", value.(*anytype.MapObject))
		ego.added.Add(node)
		ego.SliceList.Add(node)
	}
	return ego.SliceList
}

/*
Inserts a new element at the specified position in the collection.
Overrides the method of the promoted field.

Parameters:
  - index - position where the element should be inserted,
  - value - element to insert.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Insert(index int, value any) anytype.List {
	_, ok := value.(anytype.Object)
	if !ok {
		panic("Only object can be inserted to the collection.")
	}
	ego.added.Add(value)
	return ego.SliceList.Insert(index, value)
}

/*
Replaces an existing element with a new one.
Overrides the method of the promoted field.

Parameters:
  - index - position of the element which should be replaced,
  - value - new element.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Replace(index int, value any) anytype.List {
	ego.Delete(index)
	return ego.Insert(index, value)
}

/*
Deletes the elements at the specified positions in the collection.
Overrides the method of the promoted field.

Parameters:
  - indexes... - any amount of positions of the elements to delete.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Delete(indexes ...int) anytype.List {
	for index := range indexes {
		elem := ego.Get(index)
		if ego.added.Contains(elem) {
			ego.added.Delete(ego.added.IndexOf(elem))
		} else {
			if ego.modified.Contains(elem) {
				ego.modified.Delete(ego.modified.IndexOf(elem))
			}
			node, ok := elem.(*node)
			if ok {
				ego.deleted.Add(node.id)
			}
		}
	}
	return ego.SliceList.Delete(indexes...)
}

/*
Deletes the last element in the collection.
Overrides the method of the promoted field.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Pop() anytype.List {
	return ego.Delete(ego.Count() - 1)
}

/*
Deletes all elements in the collection.
Overrides the method of the promoted field.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Clear() anytype.List {
	for i := ego.Count() - 1; i >= 0; i-- {
		ego.Delete(i)
	}
	return ego.SliceList.Clear()
}

/*
Commits all changes to the database.

Returns:
  - updated list (promoted field).
*/
func (ego *collection) Commit() anytype.List {

	// Commiting added elements
	if !ego.added.Empty() {
		ego.added.
			ForEachObject(func(x anytype.Object) {
				template := x.(*node).template()
				result, err := ego.conn.Query(`CREATE (n:`+ego.label+template+`) RETURN elementId(n)`, x)
				if err != nil {
					panic(err)
				}
				id := result.GetObject(0).GetString("elementId(n)")
				x.(*node).id = id
			}).
			Clear()
	}

	// Commiting deleted elements
	if !ego.deleted.Empty() {
		ego.deleted.
			ForEachString(func(id string) {
				_, err := ego.conn.Query(`
					MATCH (n:`+ego.label+`)
					WHERE elementId(n) = $id
					DELETE n
				`, anytype.NewObject("id", id))
				if err != nil {
					panic(err)
				}
			}).
			Clear()
	}

	// Commiting modified elements
	if !ego.modified.Empty() {
		ego.modified.
			ForEachObject(func(x anytype.Object) { x.(*node).Commit() }).
			Clear()
	}

	return ego.SliceList

}
