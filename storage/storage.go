package storage

type Storage interface{
	Insert(key,value any)(any,error)
	Get(key any)(any,error)
}