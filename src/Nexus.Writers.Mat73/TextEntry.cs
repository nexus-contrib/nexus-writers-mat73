namespace Nexus.Writers
{
    public class TextEntry
    {
        public TextEntry(string path, string name, string content)
        {
            Path = path;
            Name = name;
            Content = content;
        }

        public string Path { get; private set; }
        public string Name { get; private set; }
        public string Content { get; set; }
    }
}
