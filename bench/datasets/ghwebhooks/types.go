package ghwebhooks

import "time"

// User is a nested record of Event.
type User struct {
	Login             string `avro:"login"               json:"login"`
	ID                int64  `avro:"id"                  json:"id"`
	NodeID            string `avro:"node_id"             json:"node_id"`
	AvatarURL         string `avro:"avatar_url"          json:"avatar_url"`
	GravatarID        string `avro:"gravatar_id"         json:"gravatar_id"`
	URL               string `avro:"url"                 json:"url"`
	HTMLURL           string `avro:"html_url"            json:"html_url"`
	FollowersURL      string `avro:"followers_url"       json:"followers_url"`
	FollowingURL      string `avro:"following_url"       json:"following_url"`
	GistsURL          string `avro:"gists_url"           json:"gists_url"`
	StarredURL        string `avro:"starred_url"         json:"starred_url"`
	SubscriptionsURL  string `avro:"subscriptions_url"   json:"subscriptions_url"`
	OrganizationsURL  string `avro:"organizations_url"   json:"organizations_url"`
	ReposURL          string `avro:"repos_url"           json:"repos_url"`
	EventsURL         string `avro:"events_url"          json:"events_url"`
	ReceivedEventsURL string `avro:"received_events_url" json:"received_events_url"`
	Type              string `avro:"type"                json:"type"`
	SiteAdmin         bool   `avro:"site_admin"          json:"site_admin"`
}

// License is a nested record of Event.
type License struct {
	Key    string  `avro:"key"     json:"key"`
	Name   string  `avro:"name"    json:"name"`
	SPDXID string  `avro:"spdx_id" json:"spdx_id"`
	NodeID string  `avro:"node_id" json:"node_id"`
	URL    *string `avro:"url"     json:"url"`
}

// Repository is a nested record of Event.
type Repository struct {
	ID                       int64      `avro:"id"                          json:"id"`
	NodeID                   string     `avro:"node_id"                     json:"node_id"`
	Name                     string     `avro:"name"                        json:"name"`
	FullName                 string     `avro:"full_name"                   json:"full_name"`
	Private                  bool       `avro:"private"                     json:"private"`
	Owner                    *User      `avro:"owner"                       json:"owner"`
	HTMLURL                  string     `avro:"html_url"                    json:"html_url"`
	Description              *string    `avro:"description"                 json:"description"`
	Fork                     bool       `avro:"fork"                        json:"fork"`
	URL                      string     `avro:"url"                         json:"url"`
	IssuesURL                string     `avro:"issues_url"                  json:"issues_url"`
	PullsURL                 string     `avro:"pulls_url"                   json:"pulls_url"`
	CommitsURL               string     `avro:"commits_url"                 json:"commits_url"`
	GitURL                   string     `avro:"git_url"                     json:"git_url"`
	SSHURL                   string     `avro:"ssh_url"                     json:"ssh_url"`
	CloneURL                 string     `avro:"clone_url"                   json:"clone_url"`
	Homepage                 *string    `avro:"homepage"                    json:"homepage"`
	Size                     int32      `avro:"size"                        json:"size"`
	StargazersCount          int32      `avro:"stargazers_count"            json:"stargazers_count"`
	WatchersCount            int32      `avro:"watchers_count"              json:"watchers_count"`
	Language                 *string    `avro:"language"                    json:"language"`
	ForksCount               int32      `avro:"forks_count"                 json:"forks_count"`
	OpenIssuesCount          int32      `avro:"open_issues_count"           json:"open_issues_count"`
	License                  *License   `avro:"license"                     json:"license"`
	Topics                   []string   `avro:"topics"                      json:"topics"`
	Visibility               *string    `avro:"visibility"                  json:"visibility"`
	DefaultBranch            string     `avro:"default_branch"              json:"default_branch"`
	Archived                 bool       `avro:"archived"                    json:"archived"`
	Disabled                 *bool      `avro:"disabled"                    json:"disabled"`
	HasIssues                bool       `avro:"has_issues"                  json:"has_issues"`
	IsTemplate               *bool      `avro:"is_template"                 json:"is_template"`
	WebCommitSignoffRequired *bool      `avro:"web_commit_signoff_required" json:"web_commit_signoff_required"`
	CreatedAt                *time.Time `avro:"created_at"                  json:"created_at"`
	UpdatedAt                *time.Time `avro:"updated_at"                  json:"updated_at"`
	PushedAt                 *time.Time `avro:"pushed_at"                   json:"pushed_at"`
}

// Organization is a nested record of Event.
type Organization struct {
	Login       string  `avro:"login"       json:"login"`
	ID          int64   `avro:"id"          json:"id"`
	NodeID      string  `avro:"node_id"     json:"node_id"`
	URL         string  `avro:"url"         json:"url"`
	AvatarURL   string  `avro:"avatar_url"  json:"avatar_url"`
	ReposURL    string  `avro:"repos_url"   json:"repos_url"`
	MembersURL  string  `avro:"members_url" json:"members_url"`
	Description *string `avro:"description" json:"description"`
}

// Installation is a nested record of Event.
type Installation struct {
	ID                  int64             `avro:"id"                   json:"id"`
	NodeID              *string           `avro:"node_id"              json:"node_id"`
	AppID               *int64            `avro:"app_id"               json:"app_id"`
	AppSlug             *string           `avro:"app_slug"             json:"app_slug"`
	TargetID            *int64            `avro:"target_id"            json:"target_id"`
	TargetType          *string           `avro:"target_type"          json:"target_type"`
	RepositorySelection *string           `avro:"repository_selection" json:"repository_selection"`
	Account             *User             `avro:"account"              json:"account"`
	HTMLURL             *string           `avro:"html_url"             json:"html_url"`
	AccessTokensURL     *string           `avro:"access_tokens_url"    json:"access_tokens_url"`
	Events              []string          `avro:"events"               json:"events"`
	Permissions         map[string]string `avro:"permissions"          json:"permissions"`
	CreatedAt           *time.Time        `avro:"created_at"           json:"created_at"`
	UpdatedAt           *time.Time        `avro:"updated_at"           json:"updated_at"`
	SuspendedAt         *time.Time        `avro:"suspended_at"         json:"suspended_at"`
}

// Label is a nested record of Event.
type Label struct {
	ID          int64   `avro:"id"          json:"id"`
	NodeID      string  `avro:"node_id"     json:"node_id"`
	URL         string  `avro:"url"         json:"url"`
	Name        string  `avro:"name"        json:"name"`
	Color       string  `avro:"color"       json:"color"`
	Description *string `avro:"description" json:"description"`
	Default     bool    `avro:"default"     json:"default"`
}

// Reactions is a nested record of Event.
type Reactions struct {
	URL        string `avro:"url"         json:"url"`
	TotalCount int32  `avro:"total_count" json:"total_count"`
	PlusOne    int32  `avro:"plus_one"    json:"+1"`
	MinusOne   int32  `avro:"minus_one"   json:"-1"`
	Laugh      int32  `avro:"laugh"       json:"laugh"`
	Hooray     int32  `avro:"hooray"      json:"hooray"`
	Confused   int32  `avro:"confused"    json:"confused"`
	Heart      int32  `avro:"heart"       json:"heart"`
	Rocket     int32  `avro:"rocket"      json:"rocket"`
	Eyes       int32  `avro:"eyes"        json:"eyes"`
}

// Milestone is a nested record of Event.
type Milestone struct {
	ID           int64      `avro:"id"            json:"id"`
	NodeID       string     `avro:"node_id"       json:"node_id"`
	Number       int32      `avro:"number"        json:"number"`
	URL          string     `avro:"url"           json:"url"`
	HTMLURL      string     `avro:"html_url"      json:"html_url"`
	Title        string     `avro:"title"         json:"title"`
	Description  *string    `avro:"description"   json:"description"`
	Creator      *User      `avro:"creator"       json:"creator"`
	OpenIssues   int32      `avro:"open_issues"   json:"open_issues"`
	ClosedIssues int32      `avro:"closed_issues" json:"closed_issues"`
	State        string     `avro:"state"         json:"state"`
	CreatedAt    time.Time  `avro:"created_at"    json:"created_at"`
	UpdatedAt    time.Time  `avro:"updated_at"    json:"updated_at"`
	ClosedAt     *time.Time `avro:"closed_at"     json:"closed_at"`
	DueOn        *time.Time `avro:"due_on"        json:"due_on"`
}

// Issue is a nested record of Event.
type Issue struct {
	ID                int64      `avro:"id"                 json:"id"`
	NodeID            string     `avro:"node_id"            json:"node_id"`
	Number            int32      `avro:"number"             json:"number"`
	URL               string     `avro:"url"                json:"url"`
	RepositoryURL     string     `avro:"repository_url"     json:"repository_url"`
	HTMLURL           string     `avro:"html_url"           json:"html_url"`
	Title             string     `avro:"title"              json:"title"`
	User              *User      `avro:"user"               json:"user"`
	State             *string    `avro:"state"              json:"state"`
	Locked            *bool      `avro:"locked"             json:"locked"`
	Draft             *bool      `avro:"draft"              json:"draft"`
	Assignee          *User      `avro:"assignee"           json:"assignee"`
	Assignees         []User     `avro:"assignees"          json:"assignees"`
	Labels            []Label    `avro:"labels"             json:"labels"`
	Milestone         *Milestone `avro:"milestone"          json:"milestone"`
	Comments          int32      `avro:"comments"           json:"comments"`
	CreatedAt         time.Time  `avro:"created_at"         json:"created_at"`
	UpdatedAt         time.Time  `avro:"updated_at"         json:"updated_at"`
	ClosedAt          *time.Time `avro:"closed_at"          json:"closed_at"`
	AuthorAssociation string     `avro:"author_association" json:"author_association"`
	ActiveLockReason  *string    `avro:"active_lock_reason" json:"active_lock_reason"`
	Body              *string    `avro:"body"               json:"body"`
	Reactions         *Reactions `avro:"reactions"          json:"reactions"`
}

// Comment is a nested record of Event.
type Comment struct {
	ID                  int64      `avro:"id"                     json:"id"`
	NodeID              string     `avro:"node_id"                json:"node_id"`
	URL                 *string    `avro:"url"                    json:"url"`
	HTMLURL             string     `avro:"html_url"               json:"html_url"`
	User                *User      `avro:"user"                   json:"user"`
	CreatedAt           time.Time  `avro:"created_at"             json:"created_at"`
	UpdatedAt           time.Time  `avro:"updated_at"             json:"updated_at"`
	AuthorAssociation   string     `avro:"author_association"     json:"author_association"`
	Body                string     `avro:"body"                   json:"body"`
	Path                *string    `avro:"path"                   json:"path"`
	Position            *int32     `avro:"position"               json:"position"`
	Line                *int32     `avro:"line"                   json:"line"`
	CommitID            *string    `avro:"commit_id"              json:"commit_id"`
	PullRequestReviewID *int64     `avro:"pull_request_review_id" json:"pull_request_review_id"`
	Reactions           *Reactions `avro:"reactions"              json:"reactions"`
}

// Ref is a nested record of Event.
type Ref struct {
	Label *string     `avro:"label" json:"label"`
	Ref   string      `avro:"ref"   json:"ref"`
	SHA   string      `avro:"sha"   json:"sha"`
	User  *User       `avro:"user"  json:"user"`
	Repo  *Repository `avro:"repo"  json:"repo"`
}

// PullRequest is a nested record of Event.
type PullRequest struct {
	ID                 int64      `avro:"id"                  json:"id"`
	NodeID             string     `avro:"node_id"             json:"node_id"`
	Number             int32      `avro:"number"              json:"number"`
	URL                string     `avro:"url"                 json:"url"`
	HTMLURL            string     `avro:"html_url"            json:"html_url"`
	DiffURL            string     `avro:"diff_url"            json:"diff_url"`
	PatchURL           string     `avro:"patch_url"           json:"patch_url"`
	IssueURL           string     `avro:"issue_url"           json:"issue_url"`
	CommitsURL         string     `avro:"commits_url"         json:"commits_url"`
	StatusesURL        string     `avro:"statuses_url"        json:"statuses_url"`
	State              string     `avro:"state"               json:"state"`
	Locked             bool       `avro:"locked"              json:"locked"`
	Draft              *bool      `avro:"draft"               json:"draft"`
	Title              string     `avro:"title"               json:"title"`
	User               *User      `avro:"user"                json:"user"`
	Body               *string    `avro:"body"                json:"body"`
	CreatedAt          time.Time  `avro:"created_at"          json:"created_at"`
	UpdatedAt          time.Time  `avro:"updated_at"          json:"updated_at"`
	ClosedAt           *time.Time `avro:"closed_at"           json:"closed_at"`
	MergedAt           *time.Time `avro:"merged_at"           json:"merged_at"`
	MergeCommitSHA     *string    `avro:"merge_commit_sha"    json:"merge_commit_sha"`
	Assignee           *User      `avro:"assignee"            json:"assignee"`
	Assignees          []User     `avro:"assignees"           json:"assignees"`
	RequestedReviewers []User     `avro:"requested_reviewers" json:"requested_reviewers"`
	Labels             []Label    `avro:"labels"              json:"labels"`
	Milestone          *Milestone `avro:"milestone"           json:"milestone"`
	Head               Ref        `avro:"head"                json:"head"`
	Base               Ref        `avro:"base"                json:"base"`
	AuthorAssociation  string     `avro:"author_association"  json:"author_association"`
	Merged             *bool      `avro:"merged"              json:"merged"`
	Mergeable          *bool      `avro:"mergeable"           json:"mergeable"`
	MergeableState     *string    `avro:"mergeable_state"     json:"mergeable_state"`
	Comments           *int32     `avro:"comments"            json:"comments"`
	ReviewComments     *int32     `avro:"review_comments"     json:"review_comments"`
	Commits            *int32     `avro:"commits"             json:"commits"`
	Additions          *int32     `avro:"additions"           json:"additions"`
	Deletions          *int32     `avro:"deletions"           json:"deletions"`
	ChangedFiles       *int32     `avro:"changed_files"       json:"changed_files"`
}

// Release is a nested record of Event.
type Release struct {
	ID              int64     `avro:"id"               json:"id"`
	NodeID          string    `avro:"node_id"          json:"node_id"`
	URL             string    `avro:"url"              json:"url"`
	HTMLURL         string    `avro:"html_url"         json:"html_url"`
	UploadURL       string    `avro:"upload_url"       json:"upload_url"`
	TagName         string    `avro:"tag_name"         json:"tag_name"`
	TargetCommitish string    `avro:"target_commitish" json:"target_commitish"`
	Name            *string   `avro:"name"             json:"name"`
	Draft           bool      `avro:"draft"            json:"draft"`
	Prerelease      bool      `avro:"prerelease"       json:"prerelease"`
	CreatedAt       time.Time `avro:"created_at"       json:"created_at"`
	PublishedAt     time.Time `avro:"published_at"     json:"published_at"`
	Author          *User     `avro:"author"           json:"author"`
	Body            *string   `avro:"body"             json:"body"`
}

// Author is a nested record of Event.
type Author struct {
	Name     string  `avro:"name"     json:"name"`
	Email    *string `avro:"email"    json:"email"`
	Username *string `avro:"username" json:"username"`
}

// Commit is a nested record of Event.
type Commit struct {
	ID        string    `avro:"id"        json:"id"`
	TreeID    string    `avro:"tree_id"   json:"tree_id"`
	Distinct  bool      `avro:"distinct"  json:"distinct"`
	Message   string    `avro:"message"   json:"message"`
	Timestamp time.Time `avro:"timestamp" json:"timestamp"`
	URL       string    `avro:"url"       json:"url"`
	Author    Author    `avro:"author"    json:"author"`
	Committer Author    `avro:"committer" json:"committer"`
	Added     []string  `avro:"added"     json:"added"`
	Removed   []string  `avro:"removed"   json:"removed"`
	Modified  []string  `avro:"modified"  json:"modified"`
}

// Pusher is a nested record of Event.
type Pusher struct {
	Name  string  `avro:"name"  json:"name"`
	Email *string `avro:"email" json:"email"`
}

// Event is the canonical record: one webhook delivery with the envelope shared by most events and the payloads of common ones.
type Event struct {
	Event        string        `avro:"event"         json:"event"`
	Action       *string       `avro:"action"        json:"action"`
	Sender       *User         `avro:"sender"        json:"sender"`
	Repository   *Repository   `avro:"repository"    json:"repository"`
	Organization *Organization `avro:"organization"  json:"organization"`
	Installation *Installation `avro:"installation"  json:"installation"`
	Issue        *Issue        `avro:"issue"         json:"issue"`
	Comment      *Comment      `avro:"comment"       json:"comment"`
	PullRequest  *PullRequest  `avro:"pull_request"  json:"pull_request"`
	Release      *Release      `avro:"release"       json:"release"`
	Label        *Label        `avro:"label"         json:"label"`
	Number       *int32        `avro:"number"        json:"number"`
	Ref          *string       `avro:"ref"           json:"ref"`
	RefType      *string       `avro:"ref_type"      json:"ref_type"`
	MasterBranch *string       `avro:"master_branch" json:"master_branch"`
	Description  *string       `avro:"description"   json:"description"`
	PusherType   *string       `avro:"pusher_type"   json:"pusher_type"`
	Before       *string       `avro:"before"        json:"before"`
	After        *string       `avro:"after"         json:"after"`
	Created      *bool         `avro:"created"       json:"created"`
	Deleted      *bool         `avro:"deleted"       json:"deleted"`
	Forced       *bool         `avro:"forced"        json:"forced"`
	BaseRef      *string       `avro:"base_ref"      json:"base_ref"`
	Compare      *string       `avro:"compare"       json:"compare"`
	Commits      []Commit      `avro:"commits"       json:"commits"`
	HeadCommit   *Commit       `avro:"head_commit"   json:"head_commit"`
	Pusher       *Pusher       `avro:"pusher"        json:"pusher"`
}
