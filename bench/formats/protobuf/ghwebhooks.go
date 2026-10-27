package protobuf

import (
	"time"

	"github.com/iskorotkov/avro/bench/datasets/ghwebhooks"
)

func ghMillis(t time.Time) int64 { return t.UnixMilli() }

func ghMillisPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	ms := t.UnixMilli()

	return &ms
}

func ghSlice[T, M any](in []T, convert func(*T) *M) []*M {
	out := make([]*M, len(in))
	for i := range in {
		out[i] = convert(&in[i])
	}

	return out
}

func ghUser(x *ghwebhooks.User) *GhUser {
	if x == nil {
		return nil
	}

	return &GhUser{
		Login:             x.Login,
		Id:                x.ID,
		NodeId:            x.NodeID,
		AvatarUrl:         x.AvatarURL,
		GravatarId:        x.GravatarID,
		Url:               x.URL,
		HtmlUrl:           x.HTMLURL,
		FollowersUrl:      x.FollowersURL,
		FollowingUrl:      x.FollowingURL,
		GistsUrl:          x.GistsURL,
		StarredUrl:        x.StarredURL,
		SubscriptionsUrl:  x.SubscriptionsURL,
		OrganizationsUrl:  x.OrganizationsURL,
		ReposUrl:          x.ReposURL,
		EventsUrl:         x.EventsURL,
		ReceivedEventsUrl: x.ReceivedEventsURL,
		Type:              x.Type,
		SiteAdmin:         x.SiteAdmin,
	}
}

func ghLicense(x *ghwebhooks.License) *GhLicense {
	if x == nil {
		return nil
	}

	return &GhLicense{
		Key:    x.Key,
		Name:   x.Name,
		SpdxId: x.SPDXID,
		NodeId: x.NodeID,
		Url:    x.URL,
	}
}

func ghRepository(x *ghwebhooks.Repository) *GhRepository {
	if x == nil {
		return nil
	}

	return &GhRepository{
		Id:                       x.ID,
		NodeId:                   x.NodeID,
		Name:                     x.Name,
		FullName:                 x.FullName,
		Private:                  x.Private,
		Owner:                    ghUser(x.Owner),
		HtmlUrl:                  x.HTMLURL,
		Description:              x.Description,
		Fork:                     x.Fork,
		Url:                      x.URL,
		IssuesUrl:                x.IssuesURL,
		PullsUrl:                 x.PullsURL,
		CommitsUrl:               x.CommitsURL,
		GitUrl:                   x.GitURL,
		SshUrl:                   x.SSHURL,
		CloneUrl:                 x.CloneURL,
		Homepage:                 x.Homepage,
		Size:                     x.Size,
		StargazersCount:          x.StargazersCount,
		WatchersCount:            x.WatchersCount,
		Language:                 x.Language,
		ForksCount:               x.ForksCount,
		OpenIssuesCount:          x.OpenIssuesCount,
		License:                  ghLicense(x.License),
		Topics:                   x.Topics,
		Visibility:               x.Visibility,
		DefaultBranch:            x.DefaultBranch,
		Archived:                 x.Archived,
		Disabled:                 x.Disabled,
		HasIssues:                x.HasIssues,
		IsTemplate:               x.IsTemplate,
		WebCommitSignoffRequired: x.WebCommitSignoffRequired,
		CreatedAt:                ghMillisPtr(x.CreatedAt),
		UpdatedAt:                ghMillisPtr(x.UpdatedAt),
		PushedAt:                 ghMillisPtr(x.PushedAt),
	}
}

func ghOrganization(x *ghwebhooks.Organization) *GhOrganization {
	if x == nil {
		return nil
	}

	return &GhOrganization{
		Login:       x.Login,
		Id:          x.ID,
		NodeId:      x.NodeID,
		Url:         x.URL,
		AvatarUrl:   x.AvatarURL,
		ReposUrl:    x.ReposURL,
		MembersUrl:  x.MembersURL,
		Description: x.Description,
	}
}

func ghInstallation(x *ghwebhooks.Installation) *GhInstallation {
	if x == nil {
		return nil
	}

	return &GhInstallation{
		Id:                  x.ID,
		NodeId:              x.NodeID,
		AppId:               x.AppID,
		AppSlug:             x.AppSlug,
		TargetId:            x.TargetID,
		TargetType:          x.TargetType,
		RepositorySelection: x.RepositorySelection,
		Account:             ghUser(x.Account),
		HtmlUrl:             x.HTMLURL,
		AccessTokensUrl:     x.AccessTokensURL,
		Events:              x.Events,
		Permissions:         x.Permissions,
		CreatedAt:           ghMillisPtr(x.CreatedAt),
		UpdatedAt:           ghMillisPtr(x.UpdatedAt),
		SuspendedAt:         ghMillisPtr(x.SuspendedAt),
	}
}

func ghLabel(x *ghwebhooks.Label) *GhLabel {
	if x == nil {
		return nil
	}

	return &GhLabel{
		Id:          x.ID,
		NodeId:      x.NodeID,
		Url:         x.URL,
		Name:        x.Name,
		Color:       x.Color,
		Description: x.Description,
		Default:     x.Default,
	}
}

func ghReactions(x *ghwebhooks.Reactions) *GhReactions {
	if x == nil {
		return nil
	}

	return &GhReactions{
		Url:        x.URL,
		TotalCount: x.TotalCount,
		PlusOne:    x.PlusOne,
		MinusOne:   x.MinusOne,
		Laugh:      x.Laugh,
		Hooray:     x.Hooray,
		Confused:   x.Confused,
		Heart:      x.Heart,
		Rocket:     x.Rocket,
		Eyes:       x.Eyes,
	}
}

func ghMilestone(x *ghwebhooks.Milestone) *GhMilestone {
	if x == nil {
		return nil
	}

	return &GhMilestone{
		Id:           x.ID,
		NodeId:       x.NodeID,
		Number:       x.Number,
		Url:          x.URL,
		HtmlUrl:      x.HTMLURL,
		Title:        x.Title,
		Description:  x.Description,
		Creator:      ghUser(x.Creator),
		OpenIssues:   x.OpenIssues,
		ClosedIssues: x.ClosedIssues,
		State:        x.State,
		CreatedAt:    ghMillis(x.CreatedAt),
		UpdatedAt:    ghMillis(x.UpdatedAt),
		ClosedAt:     ghMillisPtr(x.ClosedAt),
		DueOn:        ghMillisPtr(x.DueOn),
	}
}

func ghIssue(x *ghwebhooks.Issue) *GhIssue {
	if x == nil {
		return nil
	}

	return &GhIssue{
		Id:                x.ID,
		NodeId:            x.NodeID,
		Number:            x.Number,
		Url:               x.URL,
		RepositoryUrl:     x.RepositoryURL,
		HtmlUrl:           x.HTMLURL,
		Title:             x.Title,
		User:              ghUser(x.User),
		State:             x.State,
		Locked:            x.Locked,
		Draft:             x.Draft,
		Assignee:          ghUser(x.Assignee),
		Assignees:         ghSlice(x.Assignees, ghUser),
		Labels:            ghSlice(x.Labels, ghLabel),
		Milestone:         ghMilestone(x.Milestone),
		Comments:          x.Comments,
		CreatedAt:         ghMillis(x.CreatedAt),
		UpdatedAt:         ghMillis(x.UpdatedAt),
		ClosedAt:          ghMillisPtr(x.ClosedAt),
		AuthorAssociation: x.AuthorAssociation,
		ActiveLockReason:  x.ActiveLockReason,
		Body:              x.Body,
		Reactions:         ghReactions(x.Reactions),
	}
}

func ghComment(x *ghwebhooks.Comment) *GhComment {
	if x == nil {
		return nil
	}

	return &GhComment{
		Id:                  x.ID,
		NodeId:              x.NodeID,
		Url:                 x.URL,
		HtmlUrl:             x.HTMLURL,
		User:                ghUser(x.User),
		CreatedAt:           ghMillis(x.CreatedAt),
		UpdatedAt:           ghMillis(x.UpdatedAt),
		AuthorAssociation:   x.AuthorAssociation,
		Body:                x.Body,
		Path:                x.Path,
		Position:            x.Position,
		Line:                x.Line,
		CommitId:            x.CommitID,
		PullRequestReviewId: x.PullRequestReviewID,
		Reactions:           ghReactions(x.Reactions),
	}
}

func ghRef(x *ghwebhooks.Ref) *GhRef {
	if x == nil {
		return nil
	}

	return &GhRef{
		Label: x.Label,
		Ref:   x.Ref,
		Sha:   x.SHA,
		User:  ghUser(x.User),
		Repo:  ghRepository(x.Repo),
	}
}

func ghPullRequest(x *ghwebhooks.PullRequest) *GhPullRequest {
	if x == nil {
		return nil
	}

	return &GhPullRequest{
		Id:                 x.ID,
		NodeId:             x.NodeID,
		Number:             x.Number,
		Url:                x.URL,
		HtmlUrl:            x.HTMLURL,
		DiffUrl:            x.DiffURL,
		PatchUrl:           x.PatchURL,
		IssueUrl:           x.IssueURL,
		CommitsUrl:         x.CommitsURL,
		StatusesUrl:        x.StatusesURL,
		State:              x.State,
		Locked:             x.Locked,
		Draft:              x.Draft,
		Title:              x.Title,
		User:               ghUser(x.User),
		Body:               x.Body,
		CreatedAt:          ghMillis(x.CreatedAt),
		UpdatedAt:          ghMillis(x.UpdatedAt),
		ClosedAt:           ghMillisPtr(x.ClosedAt),
		MergedAt:           ghMillisPtr(x.MergedAt),
		MergeCommitSha:     x.MergeCommitSHA,
		Assignee:           ghUser(x.Assignee),
		Assignees:          ghSlice(x.Assignees, ghUser),
		RequestedReviewers: ghSlice(x.RequestedReviewers, ghUser),
		Labels:             ghSlice(x.Labels, ghLabel),
		Milestone:          ghMilestone(x.Milestone),
		Head:               ghRef(&x.Head),
		Base:               ghRef(&x.Base),
		AuthorAssociation:  x.AuthorAssociation,
		Merged:             x.Merged,
		Mergeable:          x.Mergeable,
		MergeableState:     x.MergeableState,
		Comments:           x.Comments,
		ReviewComments:     x.ReviewComments,
		Commits:            x.Commits,
		Additions:          x.Additions,
		Deletions:          x.Deletions,
		ChangedFiles:       x.ChangedFiles,
	}
}

func ghRelease(x *ghwebhooks.Release) *GhRelease {
	if x == nil {
		return nil
	}

	return &GhRelease{
		Id:              x.ID,
		NodeId:          x.NodeID,
		Url:             x.URL,
		HtmlUrl:         x.HTMLURL,
		UploadUrl:       x.UploadURL,
		TagName:         x.TagName,
		TargetCommitish: x.TargetCommitish,
		Name:            x.Name,
		Draft:           x.Draft,
		Prerelease:      x.Prerelease,
		CreatedAt:       ghMillis(x.CreatedAt),
		PublishedAt:     ghMillis(x.PublishedAt),
		Author:          ghUser(x.Author),
		Body:            x.Body,
	}
}

func ghAuthor(x *ghwebhooks.Author) *GhAuthor {
	if x == nil {
		return nil
	}

	return &GhAuthor{
		Name:     x.Name,
		Email:    x.Email,
		Username: x.Username,
	}
}

func ghCommit(x *ghwebhooks.Commit) *GhCommit {
	if x == nil {
		return nil
	}

	return &GhCommit{
		Id:        x.ID,
		TreeId:    x.TreeID,
		Distinct:  x.Distinct,
		Message:   x.Message,
		Timestamp: ghMillis(x.Timestamp),
		Url:       x.URL,
		Author:    ghAuthor(&x.Author),
		Committer: ghAuthor(&x.Committer),
		Added:     x.Added,
		Removed:   x.Removed,
		Modified:  x.Modified,
	}
}

func ghPusher(x *ghwebhooks.Pusher) *GhPusher {
	if x == nil {
		return nil
	}

	return &GhPusher{
		Name:  x.Name,
		Email: x.Email,
	}
}

func fromGhEvent(x *ghwebhooks.Event) *GhEvent {
	if x == nil {
		return nil
	}

	return &GhEvent{
		Event:        x.Event,
		Action:       x.Action,
		Sender:       ghUser(x.Sender),
		Repository:   ghRepository(x.Repository),
		Organization: ghOrganization(x.Organization),
		Installation: ghInstallation(x.Installation),
		Issue:        ghIssue(x.Issue),
		Comment:      ghComment(x.Comment),
		PullRequest:  ghPullRequest(x.PullRequest),
		Release:      ghRelease(x.Release),
		Label:        ghLabel(x.Label),
		Number:       x.Number,
		Ref:          x.Ref,
		RefType:      x.RefType,
		MasterBranch: x.MasterBranch,
		Description:  x.Description,
		PusherType:   x.PusherType,
		Before:       x.Before,
		After:        x.After,
		Created:      x.Created,
		Deleted:      x.Deleted,
		Forced:       x.Forced,
		BaseRef:      x.BaseRef,
		Compare:      x.Compare,
		Commits:      ghSlice(x.Commits, ghCommit),
		HeadCommit:   ghCommit(x.HeadCommit),
		Pusher:       ghPusher(x.Pusher),
	}
}
