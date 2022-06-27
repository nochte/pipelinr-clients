package retry

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDo(t *testing.T) {
	Convey("Do", t, func() {
		Convey("always failing", func() {
			i := 0
			st := time.Now()
			er := Do(func() error {
				i++
				return errors.New("never succeeds")
			},
				5,
				time.Millisecond*100)
			So(er, ShouldNotBeNil)
			So(i, ShouldEqual, 5)
			So(time.Since(st), ShouldBeGreaterThan, time.Millisecond*1500)
			So(time.Since(st), ShouldBeLessThan, time.Millisecond*1600)
		})

		Convey("succeeds eventually", func() {
			i := 0
			st := time.Now()
			er := Do(func() error {
				i++
				if i == 3 {
					return nil
				}
				return errors.New("failed")
			}, 5, time.Millisecond*100)
			So(er, ShouldBeNil)
			So(i, ShouldEqual, 3)
			So(time.Since(st), ShouldBeGreaterThan, time.Millisecond*300)
			So(time.Since(st), ShouldBeLessThan, time.Millisecond*400)
		})
	})

}
