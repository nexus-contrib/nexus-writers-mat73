namespace Nexus.Writers;

public class TextEntry(string path, string name, string content)
{
    public string Path { get; private set; } = path;
    public string Name { get; private set; } = name;
    public string Content { get; set; } = content;
}
