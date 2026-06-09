package in_memory_cache

type Cache struct {
	m map[string]string
}

func (c *Cache) Set(key, value string) {
	if c.m != nil {
		c.m[key] = value
		return
	}
	cache := make(map[string]string)
	cache[key] = value
	c.m = cache

}

func (c *Cache) Get(key string) (value string, ok bool) {
	return c.m[key], true
}
func (c *Cache) Delete(key string) {
	if c.m != nil {
		delete(c.m, key)
	}
}

func (c *Cache) Clear() {
	if c.m != nil {
		clear(c.m)
	}
}

func main() {
	var c Cache
	c.Set("a", "1")
	c.Get("a")
}
