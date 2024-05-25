package helper

type Paging struct {
	Page  int `json:"page" form:"page"`
	Limit int `json:"limit" form:"limit"`
	Total int `json:"total" form:""`
}

func (p *Paging) Process() {
	if p.Page < 1 {
		p.Page = 1
	}

	if p.Limit <= 0 {
		p.Limit = 10
	}
}
