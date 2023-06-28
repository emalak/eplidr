package eplidr

// TODO error handling
// Ideas: parse SQL error, return own error on validating data, BTW eplidr.Error implements error interface!

type ErrorCode int

type Error struct {
	Code    ErrorCode
	Message string
}

func (err Error) Error() string {
	return err.Message
}

const ()
