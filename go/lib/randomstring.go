package lib

import (
	"math/rand"
	"strings"
	"time"
)

//Lowercase and Uppercase Both
const charSet = "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GenerateRandomString generates a random alphanum string of length len
func GenerateRandomString(length int) string {
	var output strings.Builder

	for i := 0; i < length; i++ {
		random := rand.Intn(len(charSet))
		randomChar := charSet[random]
		output.WriteString(string(randomChar))
	}

	return output.String()
}
