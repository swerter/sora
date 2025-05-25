package imap

func (s *IMAPSession) Capabilities() []string {
	caps := make([]string, 0, len(s.server.caps))

	for cap := range s.server.caps {
		caps = append(caps, string(cap))
	}
	return caps
}
